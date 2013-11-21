package main

import(
  "io"
  "net"
  "fmt"
)

var serviceConn net.Conn

type Service struct{
  name          string
  address       string
  connection    net.Conn
}

func (s *Service) Connect(){ //Should return something
  conn, err := net.Dial("tcp", s.address)

  if err != nil {
      fmt.Println("[BROK] Error on connect to Service : " + s.name)
  }

  s.connection = conn
}

type Brok struct{
  service Service //Will be an Array in the future
}


func (b *Brok) Start(){
  listener, err := net.Listen("tcp", ":8666")
  if err != nil {
    fmt.Println("Error on Listening for Connections")
    return
  }

  fmt.Println("[BROK] Started at port 8666")

  for {
    connection, err := listener.Accept()
    if err != nil {
      break
    }
    fmt.Println("[BROK] Incomming Connection")
    go b.Handle(connection)
  }
}

func (b *Brok) Handle(clientConn net.Conn){
  defer clientConn.Close()

  go func(){
    io.Copy(b.service.connection, clientConn)
  }()

  io.Copy(clientConn, b.service.connection)

  b.service.connection.Close()
}

func main() {
  //
  service := Service{name: "redis", address:"localhost:6379"}
  service.Connect()

  //
  brok := Brok{service: service}
  brok.Start()

}
