package controlplane

import "time"

// this will start a goroutine that is responsble for pruning expired node periodically
func StartJanitor(s *State, interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-t.C:
				s.Prune()
			case <-stop:
				t.Stop()
				return
			}
		}
	}()
}
