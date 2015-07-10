package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
  mu       sync.Mutex
  l        net.Listener
  dead     int32 // for testing
  rpccount int32 // for testing
  me       string

                 // Your declarations here.
  current  View
  servers  map[string]time.Time
  acks     map[string]uint
}

func (vs *ViewServer) ack(server string, viewnum uint) {
  vs.acks[server] = viewnum;
}

func (vs *ViewServer) isAcked() bool {
  ack, ok := vs.acks[vs.current.Primary]
  return !ok || ack == vs.current.Viewnum
}

func (vs *ViewServer) isServerAcked(server string) bool {
  ack, ok := vs.acks[server]
  return ok && ack == vs.current.Viewnum
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  if args.Viewnum == vs.current.Viewnum {
    vs.ack(args.Me, args.Viewnum)
  } else if args.Viewnum < vs.acks[args.Me] {
    vs.ack(args.Me, args.Viewnum)
  }

  if vs.current.Viewnum == 0 {
    // set primary if we are at the very first view
    vs.current = View{Viewnum: 1, Primary: args.Me, Backup: ""}
    vs.ack(args.Me, 0)
  } else if vs.current.Primary != args.Me && vs.current.Backup == "" {
    // set backup if we don't have a backup
    vs.current.Viewnum += 1
    vs.current.Backup = args.Me
  }
  vs.servers[args.Me] = time.Now()
  reply.View = vs.current;
  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  reply.View = vs.current
  return nil
}

func (vs *ViewServer) isWorkerDead(server string) bool {
  var lastSeen, ok = vs.servers[server]
  return ok && time.Since(lastSeen) > DeadPings * PingInterval
}

func (vs *ViewServer) getLiveTertiaryServer() string {
  for tertiary, _ := range (vs.servers) {
    if vs.current.Primary != tertiary && vs.current.Backup != tertiary && !vs.isWorkerDead(tertiary) {
      return tertiary
    }
  }
  return ""
}

func (vs *ViewServer) getNextPrimaryServer() string {
  if !vs.isWorkerDead(vs.current.Backup) && vs.isServerAcked(vs.current.Backup) {
    return vs.current.Backup
  }
  return ""
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  // Your code here

  // if the backup is dead evict it, and replace it if possible
  if vs.isWorkerDead(vs.current.Backup) {
    // replace worker if we can
    vs.current.Backup = vs.getLiveTertiaryServer()
    vs.current.Viewnum += 1
  }


  if vs.isAcked() {
    // if primary is acked and but has died and the backup is good, replace the primary with the backup
    if vs.isWorkerDead(vs.current.Primary) {
      newPrimary := vs.getNextPrimaryServer() // get the backup if it's good.
      if newPrimary != "" {
        fmt.Println("Primary is dead, replacing with", newPrimary)
        vs.current.Primary = newPrimary
        newBackup := vs.getLiveTertiaryServer() // get a new backup if possible
        fmt.Println("Backup moved to primary, new backup is", newBackup)
        vs.current.Backup = newBackup
        vs.current.Viewnum += 1
      }
    }
  } else if vs.acks[vs.current.Primary] == 0 && vs.current.Viewnum > 0 {
    //Primary has restarted, swap with the backup
    vs.current = View{Viewnum: vs.current.Viewnum + 1, Primary: vs.current.Backup, Backup: vs.current.Primary}
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
  atomic.StoreInt32(&vs.dead, 1)
  vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
  return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
  return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.servers = map[string]time.Time{}
  vs.acks = map[string]uint{}

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me)
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.isdead() == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.isdead() == false {
        atomic.AddInt32(&vs.rpccount, 1)
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.isdead() == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.isdead() == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
