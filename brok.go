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

type Brok struct{
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

func (b *Brok) Start(){
  b.services = make(map[string]Service)
  b.AvailableServices()
}

func (b *Brok) AvailableServices(){
  redis := Service{ name: "redis",
                    localAddress:"localhost:8379",
                    externalAddress:"localhost:6379"}
  b.services[redis.name] = redis

  memcached := Service { name: "memcached",
                         localAddress:"localhost:20211",
                         externalAddress:"localhost:11211"}
  b.services[memcached.name] = memcached
}

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


func readConfig(){
  config, err := goconfig.ReadConfigFile("services")

  if err != nil{
    log.Fatalf("[BROK] Fail on Read Config")
  }

  sections := config.GetSections()

  for _, section := range sections {

    if section == "default" {continue}

    localAddress , _ := config.GetString(section, "binding-address")
    externalAddress , _ := config.GetString(section, "external-address")

    service := Service { name: section,
                         localAddress:localAddress,
                         externalAddress:externalAddress}
    service.Connect()
    go service.Listen()
  }

}


func main() {
  //
  readConfig()

  brok := new(Brok)
  //brok.Start()
  brok.Listen()

}
