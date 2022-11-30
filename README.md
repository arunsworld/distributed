# Go Utilities for Distributed Programming

This package contains utilities to help write distributed software.

Use Case:
* We have a service that is stateful; ie. only one instance can be running at a time.
* This could apply to a shard within a service rather than the whole service.
* Availability should be maximized in the face to planned or unplanned disruption to the service.
* We'd like to start up multiple of these services but only one (the leader) should ever be active.
* We have an etcd cluster that we can leverage for leadership election.

Solution:
* Allow multiple services to start up.
* The service registers itself for work and waits on being elected leader.
* Once granted it performs work; but only within the context of it's leadership that it monitors.
* If leadership is lost for whatever reason, it stops working and reverts to campaigning for leadership.

Code:
```go
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer cancel()

dConcurrency := distributed.NewETCDConcurrencyService(etcdURLs, appName, instanceName)
for {
    leadershipAcquired, leadershipLost := dConcurrency.RegisterLeadershipRequest(ctx)
    select {
    case <-ctx.Done():
        log.Println("stopping before leadership acquisition...")
        return
    case <-leadershipAcquired:
    }
    log.Printf("leadership acquired...")
    workCtx := distributed.NewWrappedLeaderMonitoredContext(ctx, leadershipLost)
    
    // this is where all the work happens within the workCtx - stop doing work when context is done
    run(workCtx)
    
    select {
    case <-ctx.Done():
        log.Printf("going to resign and quit")
        resignCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
        defer cancel()
        if err := dConcurrency.ResignLeadership(resignCtx); err != nil {
            log.Printf("error resigning: %v", err)
        }
        return
    case <-leadershipLost:
        log.Println("will try again after leadership loss")
    }
}
```

Considerations:
* Always call ResignLeadership before quiting a running application to avoid a leadership vaccumm.
* Resigning allows leadership to switch in the happy case within miliseconds avoiding any meaningful loss of availability.
* If an app crashes or loses network access and is unable to resign, there will be unavailability limited to the Lease TTL.
* A continuous connection to etcd is expected; it's the only way to prevent a network parition from creating multiple leaders.
* Also ensure Lease TTL is greater than Reassurance polling delay. This way a leader that has been cut off will realize it's no longer the leader before it's lease expires allowing another node to take over leadership.
* Loss of etcd itself will cause the application to be unavailable until etcd is restored.
* There should never be a case of two leaders unless a malicious actor deliberately revokes the lease of the leader in etcd. This can cause another node to become leader; but the issue will be limited to the polling delay at which point the affected leader realizes it's no longer the leader and stops work.