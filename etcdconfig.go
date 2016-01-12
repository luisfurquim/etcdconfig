package etcdconfig

import (
   "fmt"
   "regexp"
   "io/ioutil"
   "encoding/json"
   "golang.org/x/net/context"
   "github.com/luisfurquim/goose"
   etcd "github.com/coreos/etcd/client"
)


var Goose         goose.Alert
var reArrayIndex *regexp.Regexp = regexp.MustCompile("/\\[([0-9]+)\\]$")
var reMapIndex   *regexp.Regexp = regexp.MustCompile("/([^/]*)$")



func rSetConfig(path string, config map[string]interface{}, etcdcli etcd.KeysAPI) error {
   var key           string
   var key2          int
   var value, value2 interface{}
   var err           error
   var resp         *etcd.Response
   var optDir       *etcd.SetOptions
   var ctx           context.Context

   optDir = &etcd.SetOptions{Dir:true}
   ctx    = context.Background()

   resp, err = etcdcli.Set(ctx, path, "",optDir)
   if err != nil {
      Goose.Logf(1,"Error setting configuration, creating diretory.1 (%s): %s",path,err)
      Goose.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
   }

   for key, value = range config {
      switch value.(type) {
         case map[string]interface{} :
            err = rSetConfig(path + "/" + key, value.(map[string]interface{}), etcdcli)
            if err != nil {
               return err
            }
         case []interface{} :
            resp, err = etcdcli.Set(ctx, fmt.Sprintf("%s/%s",path,key), "", optDir)
            if err != nil {
               Goose.Fatalf(1,"Error setting configuration, creating diretory.2 (%s/%s): %s",path,key,err)
            }

            for key2, value2 = range value.([]interface{}) {
               switch value2.(type) {
                  case map[string]interface{} :
                     err = rSetConfig(fmt.Sprintf("%s/%s/[%d]",path,key,key2), value2.(map[string]interface{}), etcdcli)
                     if err != nil {
                        return err
                     }
                  case string :
                     resp, err = etcdcli.Set(ctx, fmt.Sprintf("%s/%s/[%d]",path,key,key2), value2.(string), nil)
                     if err != nil {
                        Goose.Fatalf(1,"Error setting configuration.1: %s",err)
                     } else {
                        // print common key info
                        Goose.Logf(1,"Configuration set. Metadata: %q\n", resp)
                     }
                  default:
                     Goose.Fatalf(1,"Invalid type: key=%s, key2=%d, value=%v",key,key2,value2)
               }
            }
         case string :
            resp, err = etcdcli.Set(ctx, path + "/" + key, value.(string), nil)
            if err != nil {
               Goose.Logf(1,"Error setting configuration.2: %s",err)
               Goose.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            } else {
               // print common key info
               Goose.Logf(5,"Configuration set. Metadata: %q", resp)
            }

         default:
            Goose.Fatalf(1,"Invalid type: key=%s, value=%v",key,value)

      }
   }

   return nil
}

func SetConfig(cfg string, etcdcli etcd.Client, key string) error {
   var err         error
   var configbuf []byte
   var config       map[string]interface{}

   configbuf, err = ioutil.ReadFile(cfg)
   if err != nil {
      Goose.Logf(1,"Error reading config file (%s)\n",err)
      return err
   }

   err = json.Unmarshal(configbuf, &config);
   if err != nil {
      Goose.Logf(1,"Error parsing config (%s)\n",err)
      return err
   }

   err = rSetConfig("/" + key,config,etcd.NewKeysAPI(etcdcli))
   if err != nil {
      Goose.Logf(1,"Error setting config cluster (%s)\n",err)
      return err
   }

   return nil
}

func rShowConfig(node *etcd.Node) error {
   var err     error
   var child  *etcd.Node

   if !node.Dir {
      Goose.Logf(1,"[%s] => %s",node.Key,node.Value)
      return nil
   }

   Goose.Logf(1,"[%s]",node.Key)
   for _, child = range node.Nodes {
     if child != nil {
        err = rShowConfig(child)
        if err != nil {
           Goose.Logf(1,"Error reading child node: %s",err)
           return err
        }
     }
   }

   return nil
}

func rGetConfig(node *etcd.Node) (interface{}, interface{}, error) {
   var err       error
   var child    *etcd.Node
   var i         int
   var data      interface{}
   var data2     interface{}
   var index     interface{}
   var index2    interface{}
   var array   []interface{}
   var matched []string

   matched = reArrayIndex.FindStringSubmatch(node.Key)
   if len(matched) > 0  {
      fmt.Sscanf(matched[1],"%d",&i)
      index = i
   } else {
      matched = reMapIndex.FindStringSubmatch(node.Key)
      if len(matched) <= 1  {
         Goose.Fatalf(1,"Error invalid index")
      }
      index = matched[1]
   }

   if !node.Dir {
      Goose.Logf(4,"[%s] => %s",node.Key,node.Value)
      return index, node.Value, nil
   }

   Goose.Logf(4,"[%s]",node.Key)
   for _, child = range node.Nodes {
      if child != nil {
         index2, data2, err = rGetConfig(child)
         if err != nil {
            Goose.Logf(1,"Error reading child node: %s",err)
            return nil, nil, err
         }
         switch index2.(type) {
            case string:
               if data == nil {
                  data = map[string]interface{}{}
               }
               data.(map[string]interface{})[index2.(string)] = data2
            case int:
               if array == nil {
                  array = make([]interface{},index2.(int)+1)
               } else if len(array) <= index2.(int) {
                  array = append(array,make([]interface{},index2.(int)-len(array)+1)...)
               }
               array[index2.(int)] = data2
               data = array
         }
      }
   }

   return index, data, nil
}


func GetConfig(etcdcli etcd.Client, key string) (interface{}, interface{}, error) {
   var err         error
   var resp       *etcd.Response

   resp, err = etcd.NewKeysAPI(etcdcli).Get(context.Background(), "/" + key, &etcd.GetOptions{Recursive:true})
   if err != nil {
      Goose.Logf(1,"Error fetching configuration: %s",err)
      return nil, nil, err
   }

   return rGetConfig(resp.Node)
}

func DeleteConfig(etcdcli etcd.Client, key string) error {
   var err         error

   _, err = etcd.NewKeysAPI(etcdcli).Delete(context.Background(), "/" + key, &etcd.DeleteOptions{Recursive:true,Dir:true})
   if err != nil {
      Goose.Logf(1,"Error deleting configuration: %s",err)
      return err
   }

   return nil
}


func SetKey(etcdcli etcd.Client, key string, value string) error {
   var err         error
   var resp         *etcd.Response
   var ctx           context.Context

   ctx    = context.Background()

   resp, err = etcd.NewKeysAPI(etcdcli).Set(ctx, "/" + key, value, nil)
   if err != nil {
      Goose.Logf(1,"Error setting configuration.2: %s",err)
      Goose.Fatalf(5,"key:%s     Metadata: %q", key, resp)
   } else {
      // print common key info
      Goose.Logf(5,"Configuration set. Metadata: %q", resp)
   }

   return nil
}

func OnUpdate(etcdCli etcd.Client, key string, fn func(val string)) {
   var kapi           etcd.KeysAPI
   var ctx            context.Context

   kapi = etcd.NewKeysAPI(etcdCli)
   ctx  = context.Background()

   go func (w etcd.Watcher) {
      var err error
      var resp         *etcd.Response

      for {
         resp, err = w.Next(ctx)
         if err == nil {
            fn(resp.Node.Value)
         } else {
            Goose.Logf(1,"Error updating config variable %s (%s)",key,err)
         }
      }
   }(kapi.Watcher(key,nil))
}


