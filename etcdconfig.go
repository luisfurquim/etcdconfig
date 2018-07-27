package etcdconfig

import (
   "io"
   "fmt"
   "sort"
   "math"
   "errors"
//   "time"
//   "reflect"
   "regexp"
//   "reflect"
   "io/ioutil"
   "encoding/json"
   "golang.org/x/net/context"
   "github.com/luisfurquim/goose"
   etcd "github.com/coreos/etcd/client"
)

type watchReq struct {
   key string
   id  string
   resp chan bool
}

type EtcdconfigG struct {
   Setter  goose.Alert
   Getter  goose.Alert
   Updater goose.Alert
}

var Goose EtcdconfigG

var reArrayIndex *regexp.Regexp = regexp.MustCompile("/\\[([0-9]+)\\]$")
var reMapIndex   *regexp.Regexp = regexp.MustCompile("/([^/]*)$")
var keysWatched map[string][]string
var wreq chan watchReq
var ErrIndex = errors.New("Error invalid index.")

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
      Goose.Setter.Logf(1,"Error setting configuration, creating diretory.1 (%s): %s",path,err)
      Goose.Setter.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
   }

   for key, value = range config {
      switch value.(type) {
         case map[string]interface{} :
            Goose.Setter.Logf(5,"Found %s/%s=%q => map[string]interface{}", path, key, value.(map[string]interface{}))
            err = rSetConfig(path + "/" + key, value.(map[string]interface{}), etcdcli)
            if err != nil {
               return err
            }
         case []interface{} :
            Goose.Setter.Logf(5,"Found %s/%s=%q => []interface{}", path, key, value.([]interface{}))
            resp, err = etcdcli.Set(ctx, fmt.Sprintf("%s/%s",path,key), "", optDir)
            if err != nil {
               Goose.Setter.Fatalf(1,"Error setting configuration, creating diretory.2 (%s/%s): %s",path,key,err)
            }

            for key2, value2 = range value.([]interface{}) {
               switch value2.(type) {
                  case map[string]interface{} :
                     Goose.Setter.Logf(5,"Found %s/%s=%q => []interface{map[string]interface{}}", path, key, value2.(map[string]interface{}))
                     err = rSetConfig(fmt.Sprintf("%s/%s/[%d]",path,key,key2), value2.(map[string]interface{}), etcdcli)
                     if err != nil {
                        return err
                     }
                  case string :
                     Goose.Setter.Logf(5,"Found %s/%s=%q => []interface{string}", path, key, value2.(string))
                     resp, err = etcdcli.Set(ctx, fmt.Sprintf("%s/%s/[%d]",path,key,key2), value2.(string), nil)
                     if err != nil {
                        Goose.Setter.Fatalf(1,"Error setting configuration.1: %s",err)
                     } else {
                        // print common key info
                        Goose.Setter.Logf(5,"Configuration set. Metadata: %q\n", resp)
                     }
                  default:
                     Goose.Setter.Fatalf(1,"Invalid type: key=%s, key2=%d, value=%v",key,key2,value2)
               }
            }
         case string :
            resp, err = etcdcli.Set(ctx, path + "/" + key, value.(string), nil)
            if err != nil {
               Goose.Setter.Logf(1,"Error setting configuration.2: %s",err)
               Goose.Setter.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            } else {
               // print common key info
               Goose.Setter.Logf(5,"Configuration.2 %s/%s=%s set. Metadata: %q", path, key, value.(string), resp)
            }

         case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
            resp, err = etcdcli.Set(ctx, path + "/" + key, fmt.Sprintf("%d",value), nil)
            if err != nil {
               Goose.Setter.Logf(1,"Error setting configuration.3: %s",err)
               Goose.Setter.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            } else {
               // print common key info
               Goose.Setter.Logf(5,"Configuration.3 %s/%s=%d set. Metadata: %q", path, key, value, resp)
            }


         case float32:
            if math.Floor(float64(value.(float32))) == float64(value.(float32)) {
               resp, err = etcdcli.Set(ctx, path + "/" + key, fmt.Sprintf("%.0f",math.Floor(float64(value.(float32)))), nil)
            } else {
               resp, err = etcdcli.Set(ctx, path + "/" + key, fmt.Sprintf("%f",value.(float32)), nil)
            }
            if err != nil {
               Goose.Setter.Logf(1,"Error setting configuration.4: %s",err)
               Goose.Setter.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            } else {
               // print common key info
               Goose.Setter.Logf(5,"Configuration.4 %s/%s=%c set. Metadata: %q", path, key, value, resp)
            }

         case float64:
            if math.Floor(value.(float64)) == value.(float64) {
               resp, err = etcdcli.Set(ctx, path + "/" + key, fmt.Sprintf("%.0f",math.Floor(value.(float64))), nil)
            } else {
               resp, err = etcdcli.Set(ctx, path + "/" + key, fmt.Sprintf("%f",value.(float64)), nil)
            }
            if err != nil {
               Goose.Setter.Logf(1,"Error setting configuration.4: %s",err)
               Goose.Setter.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            } else {
               // print common key info
               Goose.Setter.Logf(5,"Configuration.5 %s/%s=%c set. Metadata: %q", path, key, value, resp)
            }


         default:
            resp, err = etcdcli.Set(ctx, path + "/" + key, fmt.Sprintf("%s",value), nil)
            if err != nil {
               Goose.Setter.Logf(1,"Error setting configuration.5: %s",err)
               Goose.Setter.Fatalf(5,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            } else {
               // print common key info
               Goose.Setter.Logf(5,"Configuration.6 %s/%s=%s set. Metadata: %q", path, key, value, resp)
            }


//            Goose.Setter.Fatalf(1,"Invalid type: key=%s, value=%#v, valuetype:%#v",key,value,reflect.TypeOf(value))

      }
   }

   return nil
}

func Set(etcdcli etcd.KeysAPI, path, key, typ string) error {
	var err 		error
	var resp		*etcd.Response
	var ctx     context.Context
	
	ctx    = context.Background()
	switch typ {
		case "string": 
		Goose.Setter.Logf(5,"Key to set: %s\n", "/"+ path + "/" + key)
			resp, err = etcdcli.Set(ctx, "/" + path + "/" + key, "", nil)
         if err != nil {
				Goose.Setter.Logf(1,"Error setting configuration to string type: %s",err)
            Goose.Setter.Logf(1,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            return err
         } else {
               // print common key info
               Goose.Setter.Logf(1,"Configuration type string %s/%s set. Metadata: %q", path, key, resp)
         }
		case "number":
			Goose.Setter.Logf(1,"Key to set: %s\n", "/"+ path + "/" + key)
			resp, err = etcdcli.Set(ctx, "/"+ path + "/" + key, fmt.Sprintf("%d",0), nil)
         if err != nil {
				Goose.Setter.Logf(1,"Error setting configuration to number type: %s",err)
            Goose.Setter.Logf(1,"path:%s,   key:%s     Metadata: %q",path, key, resp)
         } else {
				// print common key info
            Goose.Setter.Logf(1,"Configuration type number %s/%s set. Metadata: %q", path, key, resp)
        }
		case "boolean":
			Goose.Setter.Logf(1,"Key to set: %s\n", "/"+ path + "/" + key)
			resp, err = etcdcli.Set(ctx, "/"+ path + "/" + key, fmt.Sprintf("%s","true"), nil)
         if err != nil {
				Goose.Setter.Logf(1,"Error setting configuration to boolean type: %s",err)
            Goose.Setter.Logf(1,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            return err
         } else {
				// print common key info
            Goose.Setter.Logf(1,"Configuration type boolean %s/%s set. Metadata: %q", path, key, resp)
			}
		default:
			resp, err = etcdcli.Set(ctx, "/"+ path + "/" + key, fmt.Sprintf("%s",""), nil)
         if err != nil {
				Goose.Setter.Logf(1,"Error setting configuration to default: %s",err)
            Goose.Setter.Logf(1,"path:%s,   key:%s     Metadata: %q",path, key, resp)
            return err
         } else {
               // print common key info
               Goose.Setter.Logf(1,"Configuration default %s/%s set. Metadata: %q", path, key, resp)
         }
     }//ends switch
     return nil
}

func SetConfig(cfg string, etcdcli etcd.Client, key string) error {
   var err         error
   var configbuf []byte
   var config       map[string]interface{}

   configbuf, err = ioutil.ReadFile(cfg)
   if err != nil {
      Goose.Setter.Logf(1,"Error reading config file (%s)\n",err)
      return err
   }

   err = json.Unmarshal(configbuf, &config);
   if err != nil {
      Goose.Setter.Logf(1,"Error parsing config (%s)\n",err)
      return err
   }

   err = rSetConfig("/" + key,config,etcd.NewKeysAPI(etcdcli))
   if err != nil {
      Goose.Setter.Logf(1,"Error setting config cluster (%s)\n",err)
      return err
   }

   return nil
}

func SetConfigFromReader(cfg io.Reader, etcdcli etcd.Client, key string) error {
   var err         error
   var configbuf []byte
   var config       map[string]interface{}

   configbuf, err = ioutil.ReadAll(cfg)
   if err != nil {
      Goose.Setter.Logf(1,"Error reading config file (%s)\n",err)
      return err
   }

   err = json.Unmarshal(configbuf, &config);
   if err != nil {
      Goose.Setter.Logf(1,"Error parsing config (%s)\n",err)
      return err
   }

   err = rSetConfig("/" + key,config,etcd.NewKeysAPI(etcdcli))
   if err != nil {
      Goose.Setter.Logf(1,"Error setting config cluster (%s)\n",err)
      return err
   }

   return nil
}

func SetConfigFromMap(config map[string]interface{}, etcdcli etcd.Client, key string) error {
   var err         error

   err = rSetConfig("/" + key,config,etcd.NewKeysAPI(etcdcli))
   if err != nil {
      Goose.Setter.Logf(1,"Error setting config cluster (%s)\n",err)
      return err
   }

   return nil
}

func rShowConfig(node *etcd.Node) error {
   var err     error
   var child  *etcd.Node

   if !node.Dir {
      Goose.Getter.Logf(1,"[%s] => %s",node.Key,node.Value)
      return nil
   }

   Goose.Getter.Logf(1,"[%s]",node.Key)
   for _, child = range node.Nodes {
     if child != nil {
        err = rShowConfig(child)
        if err != nil {
           Goose.Getter.Logf(1,"Error reading child node: %s",err)
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
         Goose.Getter.Logf(1,"Error invalid index")
         return nil, nil, ErrIndex
      }
      index = matched[1]
   }

   if !node.Dir {
      Goose.Getter.Logf(4,"[%s] => %s",node.Key,node.Value)
      return index, node.Value, nil
   }

   Goose.Getter.Logf(4,"[%s]",node.Key)
   for _, child = range node.Nodes {
      if child != nil {
         index2, data2, err = rGetConfig(child)
         if err != nil {
            Goose.Getter.Logf(1,"Error reading child node: %s",err)
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
      Goose.Getter.Logf(1,"Error fetching configuration for %s: %s", "/" + key, err)
      return nil, nil, err
   }

   return rGetConfig(resp.Node)
}

func DeleteConfig(etcdcli etcd.Client, key string) error {
   var err         error

   _, err = etcd.NewKeysAPI(etcdcli).Delete(context.Background(), "/" + key, &etcd.DeleteOptions{Recursive:true,Dir:true})
   if err != nil {
      Goose.Updater.Logf(1,"Error deleting configuration: %s",err)
      return err
   }

   return nil
}


func SetKey(etcdcli etcd.Client, key string, value string) error {
   var err         error
   var resp         *etcd.Response
   var ctx           context.Context

   ctx    = context.Background()
   Goose.Setter.Logf(6,"Entering SetKey /%s <- %#v", key, value)
   resp, err = etcd.NewKeysAPI(etcdcli).Set(ctx, "/" + key, value, nil)
   if err != nil {
      Goose.Setter.Logf(1,"Error setting configuration.2: %s",err)
      Goose.Setter.Logf(5,"key:%s     Metadata: %q", key, resp)
      panic(fmt.Sprintf("Error setting configuration: %s",err))
   } else {
      // print common key info
      Goose.Setter.Logf(5,"Configuration set. Metadata: %q", resp)
   }

   return nil
}


func watcherChecker(req chan watchReq) {
   var reqKey watchReq
   var ok bool
   var j int
   var kw []string

   for {
      reqKey = <-req
      if reqKey.key == "" {
         return
      }

      if kw, ok = keysWatched[reqKey.key]; !ok {
         reqKey.resp<- false
         keysWatched[reqKey.key] = []string{reqKey.id}
      }

      j = sort.Search(len(kw), func(i int) bool { return kw[i] >= reqKey.id })
      if j < len(kw) && kw[j] == reqKey.id {
         // reqKey.fn is present at kw[j]
         reqKey.resp <- true
      } else {
         reqKey.resp <- false
         // reqKey.id is not present at kw[j]
         // but j is the index where it would be inserted.
         if j==len(kw) {
            keysWatched[reqKey.key] = append(kw,reqKey.id)
         } else {
            kw = append(kw,reqKey.id)
            copy(kw[j+1:],kw[j:len(kw)-1])
            kw[j] = reqKey.id
            keysWatched[reqKey.key] = kw
         }
      }
   }
}

func chkWatcher(key string, id string) bool {
   var resp chan bool

   resp = make(chan bool,1)
   wreq<- watchReq{key:key, id:id, resp: resp}

   return <-resp
}

func OnUpdate(etcdCli etcd.Client, key string, fn func(val string), id ...interface{}) {
   var kapi           etcd.KeysAPI
   var ctx            context.Context
   var sid            string

   if len(id) > 0 {
      switch id[0].(type) {
         case string:
            sid = id[0].(string)
         default:
            Goose.Updater.Logf(2,"Ignoring wrong parameter type for key %s (%#v)",key,id[0])
            return
      }
   }

   if chkWatcher(key, sid) {
      return
   }

   kapi = etcd.NewKeysAPI(etcdCli)
   ctx  = context.Background()

   go func (w etcd.Watcher) {
      var err error
      var resp         *etcd.Response

      for {
         resp, err = w.Next(ctx)
         if err == nil {
            Goose.Updater.Logf(3,"Updating config variable %s = %s", key, resp.Node.Value)
            fn(resp.Node.Value)
         } else {
            Goose.Updater.Logf(1,"Error updating config variable %s (%s)",key,err)
         }
      }
   }(kapi.Watcher("/" + key,nil))
}

func OnUpdateIFace(etcdCli etcd.Client, key string, fn func(val interface{}), id ...interface{}) {
   var kapi           etcd.KeysAPI
   var ctx            context.Context
   var sid            string

   if len(id) > 0 {
      switch id[0].(type) {
         case string:
            sid = id[0].(string)
         default:
            Goose.Updater.Logf(2,"Ignoring wrong parameter type for key %s (%#v)",key,id[0])
            return
      }
   }

   if chkWatcher(key, sid) {
      return
   }

   kapi = etcd.NewKeysAPI(etcdCli)
   ctx  = context.Background()

   go func (w etcd.Watcher) {
      var err error
      var resp         *etcd.Response

      for {
         resp, err = w.Next(ctx)
         if err == nil {
            Goose.Updater.Logf(3,"Updating config variable %s = %s",key,resp.Node.Value)
            fn(resp.Node.Value)
         } else {
            Goose.Updater.Logf(1,"Error updating config variable %s (%s)",key,err)
         }
      }
   }(kapi.Watcher("/" + key,nil))
}

func OnUpdateTree(etcdCli etcd.Client, key string, fn func(key string, val interface{}, action string), id ...interface{}) {
   var kapi           etcd.KeysAPI
   var ctx            context.Context
   var sid            string

   if len(id) > 0 {
      switch id[0].(type) {
         case string:
            sid = id[0].(string)
         default:
            Goose.Updater.Logf(2,"Ignoring wrong parameter type for key %s (%#v)",key,id[0])
            return
      }
   }

   if chkWatcher(key, sid) {
      return
   }

   kapi = etcd.NewKeysAPI(etcdCli)
   ctx  = context.Background()

   go func (w etcd.Watcher) {
      var err error
      var resp         *etcd.Response

      for {
         resp, err = w.Next(ctx)
         if err == nil {
            Goose.Updater.Logf(3,"Updating (%s) config variable %s = %s", resp.Action, key, resp.Node.Value)
            fn(resp.Node.Key, resp.Node.Value, resp.Action)
         } else {
            Goose.Updater.Logf(1,"Error updating config variable %s (%s)",key,err)
         }
      }
   }(kapi.Watcher("/" + key,&etcd.WatcherOptions{Recursive:true}))
}



func init() {
   keysWatched = map[string][]string{}
   wreq = make(chan watchReq,1)
   go watcherChecker(wreq)
}

