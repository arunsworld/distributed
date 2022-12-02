package distributed

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arunsworld/nursery"
)

type Job struct {
	Name string
	Work func(context.Context) error
}

type ProcessorOpt func(*LeadershipBasedJobProcessor)

func NewLeadershipBasedJobProcessor(appName, instanceName string, leaseProvider LeaseProvider, jobs []Job, opts ...ProcessorOpt) *LeadershipBasedJobProcessor {
	result := &LeadershipBasedJobProcessor{
		appName:       appName,
		instanceName:  instanceName,
		leaseProvider: leaseProvider,
		jobs:          jobs,
	}
	for _, o := range opts {
		o(result)
	}
	return result
}

type LeadershipBasedJobProcessor struct {
	appName       string
	instanceName  string
	leaseProvider LeaseProvider
	jobs          []Job
	// optional
	blockOnNoJobs    bool
	startDelay       func(string) time.Duration
	voluntaryTimeout func(string) time.Duration
}

// Add this option to block the execution if there are no jobs configured
// If this option is not provided; then execution errors if no jobs are configured
func WithBlockOnNoJobs() ProcessorOpt {
	return func(p *LeadershipBasedJobProcessor) {
		p.blockOnNoJobs = true
	}
}

func WithStartDelay(gen func(string) time.Duration) ProcessorOpt {
	return func(p *LeadershipBasedJobProcessor) {
		p.startDelay = gen
	}
}

func WithVoluntaryTimeout(gen func(string) time.Duration) ProcessorOpt {
	return func(p *LeadershipBasedJobProcessor) {
		p.voluntaryTimeout = gen
	}
}

func (p *LeadershipBasedJobProcessor) AddJob(j Job) {
	p.jobs = append(p.jobs, j)
}

func (p *LeadershipBasedJobProcessor) Execute(ctx context.Context) error {
	if len(p.jobs) == 0 {
		if !p.blockOnNoJobs {
			return fmt.Errorf("error: no jobs configured")
		}
		log.Println("info: no jobs provided; will block until context is done")
		<-ctx.Done()
		return nil
	}
	if err := areJobNamesUnique(p.jobs); err != nil {
		return err
	}
	dConcurrency := NewConcurrency(p.appName, p.instanceName, p.leaseProvider)
	defer dConcurrency.Close()

	if p.voluntaryTimeout != nil {
		return p.executeWithVoluntaryTimeout(ctx, dConcurrency)
	} else {
		return p.execute(ctx, dConcurrency)
	}
}

func (p *LeadershipBasedJobProcessor) execute(ctx context.Context, dConcurrency *Concurrency) error {
	nurseryJobs := make([]nursery.ConcurrentJob, 0, len(p.jobs))
	for _, j := range p.jobs {
		job := j
		nurseryJobs = append(nurseryJobs, func(ctx context.Context, errCh chan error) {
			if p.startDelay != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(p.startDelay(job.Name)):
				}
			}
			for {
				leadershipAcquired, leadershipLost, err := dConcurrency.RegisterLeadershipRequest(job.Name)
				if err != nil {
					errCh <- err
					return
				}
				select {
				case <-leadershipAcquired:
					log.Println("leadership acquired for: ", job.Name)
					jobCtx := NewWrappedLeaderMonitoredContext(ctx, leadershipLost)
					if err := job.Work(jobCtx); err != nil {
						errCh <- err
						return
					}
					select {
					case <-ctx.Done():
						return
					default:
						log.Printf("leadership lost for: %s. will retry...", job.Name)
					}
				case <-ctx.Done():
					return
				}
			}
		})
	}
	return nursery.RunConcurrentlyWithContext(ctx, nurseryJobs...)
}

func (p *LeadershipBasedJobProcessor) executeWithVoluntaryTimeout(ctx context.Context, dConcurrency *Concurrency) error {
	nurseryJobs := make([]nursery.ConcurrentJob, 0, len(p.jobs))
	for _, j := range p.jobs {
		job := j
		nurseryJobs = append(nurseryJobs, func(ctx context.Context, errCh chan error) {
			for {
				leadershipAcquired, leadershipLost, err := dConcurrency.RegisterLeadershipRequest(job.Name)
				if err != nil {
					errCh <- err
					return
				}
				select {
				case <-leadershipAcquired:
					log.Println("leadership acquired for: ", job.Name)
					// voluntary timeout
					vctx, vcancel := context.WithTimeout(ctx, p.voluntaryTimeout(job.Name))
					jobCtx := NewWrappedLeaderMonitoredContext(vctx, leadershipLost)
					if err := job.Work(jobCtx); err != nil {
						errCh <- err
						vcancel()
						return
					}
					vcancel()
					select {
					case <-ctx.Done():
						return
					default:
					}

					select {
					case <-vctx.Done():
						rctx, rcancel := context.WithTimeout(context.Background(), time.Second*5)
						if err := dConcurrency.ResignLeadership(rctx, job.Name); err != nil {
							log.Printf("error resigning from %s: %v", job.Name, err)
						}
						rcancel()
						log.Printf("leadership voluntarily resigned for: %s. returning to leadership election...", job.Name)
					default:
						log.Printf("leadership lost for: %s. will retry...", job.Name)
					}
				case <-ctx.Done():
					return
				}
			}
		})
	}
	return nursery.RunConcurrentlyWithContext(ctx, nurseryJobs...)
}

func areJobNamesUnique(jobs []Job) error {
	names := make(map[string]struct{})
	for _, j := range jobs {
		if _, alreadyPresent := names[j.Name]; alreadyPresent {
			return fmt.Errorf("error: job with name: %s repeated", j.Name)
		}
		names[j.Name] = struct{}{}
	}
	return nil
}
