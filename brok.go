package main

import(
  "io"
  "net"
  "log"
  "strings"
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
  applicationName string
  config          map[string]string
  servicesConfig  *Config
  services        map[string]Service
}

var brok *Brok

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


func (b *Brok) Listen() {
  listenPort := strings.Join([]string{":", b.config["binding-port"]}, "")
  listener, err := net.Listen("tcp", listenPort)

  if err != nil {
    log.Printf("[BROK] Got an Error on trying Listening at %s", listenPort)
    return
  }

  log.Println("[BROK] Is Proud to start at %s", listenPort)

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

  for key, configItem := range b.servicesConfig.items{

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

func (c *Config) ReadServicesFile(configFile string){
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


func (b *Brok) ReadConfig(configFile string){
  b.config = make(map[string]string)
  config, err := goconfig.ReadConfigFile(configFile)

  if err != nil{
    log.Fatalf("[BROK] Fail on Read Config")
  }

  b.applicationName, _ = config.GetString("brok", "application-name")
  b.config["backend-type"], _ = config.GetString("brok", "backend-type")
  b.config["binding-port"], _ = config.GetString("brok", "binding-port")
  b.config["backend-host"], _ = config.GetString("backend", "host")
  b.config["backend-port"], _ = config.GetString("backend", "port")
  b.config["backend-user"], _ = config.GetString("backend", "user")
  b.config["backend-password"], _ = config.GetString("backend", "password")

}


func (backend *Backend) Watch(){
  backendHost := brok.config["backend-host"]
  backendPort := uint(6379) //Move port to dynamic attribute
  backend.consumer = redis.New()
  backend.consumer.ConnectNonBlock(backendHost,backendPort)

  //application:tag:service
  rec := make(chan []string)
  go backend.consumer.Subscribe(rec, "brok:v0.0.1:redis", "brok:v0.0.1:mysql")

  var ls []string
  for {
    ls = <- rec
    log.Printf("Channel: %s / Value : %s", ls[1], ls[2]) //strings.Join(ls, " "))
  }

}


func main() {
  //
  servicesConfig := new(Config)
  servicesConfig.ReadServicesFile("./config/services")

  //
  brok = new(Brok)
  brok.ReadConfig("./config/brok.conf")
  brok.servicesConfig = servicesConfig

  //
  backend := new(Backend)
  go backend.Watch()

  //
  brok.StartServices()
  brok.Listen()

}
