package main

import(
  "io"
  "net"
  "log"
  "github.com/msbranco/goconfig"
)


type Service struct{
  name             string
  localAddress     string
  externalAddress  string
  connection       net.Conn
}

type Config struct{
  items map[string]ConfigItem
}

type ConfigItem struct{
  bindingAddress  string
  externalAddress  string
}

type Brok struct{
  config *Config
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
      log.Println("[BROK] Error on connect to Service : " + s.name)
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
  for key, configItem := range b.config.items{

    service := Service {name: key,
                        localAddress:configItem.bindingAddress,
                        externalAddress:configItem.externalAddress}
    service.Connect()
    go service.Listen()

  }
}


func (c *Config) Read(){
  c.items = make(map[string]ConfigItem)
  config, err := goconfig.ReadConfigFile("services")

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


func main() {
  //
  config := new(Config)
  config.Read()

  //
  brok := new(Brok)
  brok.config = config

  //
  brok.StartServices()
  brok.Listen()

}
