package main

import(
  "io"
  "net"
  "log"
  "github.com/msbranco/goconfig"
  "github.com/gosexy/redis"
)

type Backend struct {
  client *redis.Client
  consumer *redis.Client
}

type Service struct{
  name             string
  localAddress     string
  externalAddress  string
  connection       net.Conn
}

type Config struct{
  applicationName  string
  items            map[string]ConfigItem
}

type ConfigItem struct{
  bindingAddress  string
  externalAddress  string
}

type Brok struct{
  config *Config
  services map[string]Service
}

func (s *Service) Listen() {
  listener, err := net.Listen("tcp", s.localAddress)
  if err != nil {
    log.Printf("[BROK] Error on Listening for Connections at %s : %s", s.localAddress, err.Error)
    return
  }

  log.Printf("[BROK] Service %s started at %s", s.name, s.localAddress)

  for {
    connection, err := listener.Accept()
    if err != nil {
      break
    }
    log.Println("[BROK] Incomming Connection")
    go s.Handle(connection)
  }

}

func (s *Service) Handle(clientConn net.Conn){
  defer clientConn.Close()

  go func(){
    io.Copy(s.connection, clientConn)
  }()

  io.Copy(clientConn, s.connection)

  s.connection.Close()
}


func (s *Service) Connect() bool{
  conn, err := net.Dial("tcp", s.externalAddress)

  if err != nil {
      log.Printf("[BROK] Error on connect to Service : %s", s.name)
      return false
  }

  s.connection = conn
  return true
}

/////////////////////////////
/////////////////////////////
/////////////////////////////
/////////////////////////////


func (b *Brok) Listen() {
  listener, err := net.Listen("tcp", ":9666")

  if err != nil {
    log.Println("[BROK] Got an Error on trying Listening at :9666")
    return
  }

  log.Println("[BROK] Is Proud to start at :9666")

  for {
    connection, err := listener.Accept()
    if err != nil {
      break
    }
    log.Println("[BROK] Incomming Connection")
    go b.Handle(connection)
  }

}

func (b *Brok) Handle(clientConn net.Conn){
  defer clientConn.Close()
}


func (b *Brok) StartServices(){
  b.services = make(map[string]Service)

  for key, configItem := range b.config.items{

    //
    service := Service {name: key,
                        localAddress:configItem.bindingAddress,
                        externalAddress:configItem.externalAddress}

    //
    b.services[key] = service

    //
    service.Connect()
    go service.Listen()

  }
}

func (c *Config) Read(configFile string){
  c.items = make(map[string]ConfigItem)
  config, err := goconfig.ReadConfigFile(configFile)

  if err != nil{
    log.Fatalf("[BROK] Fail on Read Config")
  }

  sections := config.GetSections()

  for _, section := range sections {

    if section == "default" {continue}

    localAddress , _ := config.GetString(section, "binding-address")
    externalAddress , _ := config.GetString(section, "external-address")
    c.items[section] = ConfigItem{bindingAddress:localAddress, externalAddress:externalAddress}

  }

}


func publish(){
  backend := new(Backend)
  backend.client = redis.New()
  backend.client.Connect("127.0.0.1", 6379)
  for i := 0; i < 14; i++ {
    backend.client.Publish("brok:redis:development", i)
  }

}


func (backend *Backend) Watch(){
  backend.consumer = redis.New()
  backend.consumer.ConnectNonBlock("127.0.0.1", 6379)

  //application:service:environment
  rec := make(chan []string)
  go backend.consumer.Subscribe(rec, "brok:redis:development")

  var ls []string
  for {
    ls = <- rec
    log.Println("brok:redis:development is ", ls[2]) //strings.Join(ls, " "))
  }


}

func main() {
  //
  config := new(Config)
  config.Read("./config/services")
  config.applicationName = "brok"

  //
  brok := new(Brok)
  brok.config = config

  //
  backend := new(Backend)
  go backend.Watch()

  //
  brok.StartServices()
  brok.Listen()

}
