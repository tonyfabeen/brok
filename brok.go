package main

import(
  "io"
  "net"
  "fmt"
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
    fmt.Println("[BROK] Error on Listening for Connections at ", s.localAddress)
    return
  }

  fmt.Println("[BROK] Service started at ", s.localAddress)

  for {
    connection, err := listener.Accept()
    if err != nil {
      break
    }
    fmt.Println("[BROK] Incomming Connection")
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
      fmt.Println("[BROK] Error on connect to Service : " + s.name)
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
  b.ProxyAll()
}

func (b *Brok) AvailableServices(){
  redis := Service{ name: "redis",
                    localAddress:"localhost:8379",
                    externalAddress:"localhost:6379"}
  b.services[redis.name] = redis
}

func (b *Brok) ProxyAll(){
  for key, service := range b.services {
    fmt.Println("KEY\t: ", key,
                "\nLOCAL\t: ", service.localAddress,
                "\nREMOTE\t: ", service.externalAddress)

    //
    service.Connect()
    service.Listen()
  }
}

func main() {
  //
  brok := new(Brok)
  brok.Start()

}
