package main

import(
  "io"
  "net"
  "fmt"
)

var serviceConn net.Conn

func start(){
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
    go handleConnection(connection)
  }
}

func handleConnection(connection net.Conn){
  defer connection.Close()

  go func(){
    io.Copy(serviceConn, connection)
  }()

  io.Copy(connection, serviceConn)

  serviceConn.Close()
}

func connectToService() net.Conn {
  //In this example i'm using REDIS as Service Backend
  serviceConn, err := net.Dial("tcp", "localhost:6379")

  if err != nil {
    fmt.Println("Error on connect to Service")
  }
  return serviceConn
}

func main() {
  serviceConn = connectToService()
  start()
}
