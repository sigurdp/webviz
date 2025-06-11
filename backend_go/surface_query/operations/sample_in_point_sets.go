package operations

import (
	"fmt"
	"log/slog"
	"runtime"
	"surface_query/utils"
	"surface_query/xtgeo"
	"sync"
	"time"
)

type RealSurfObjId struct {
	Realization int
	ObjectUuid  string
}

type PointSet struct {
	XCoords []float64
	YCoords []float64
}

type RealSamples struct {
	Realization   int
	SampledValues []float32
}

type PointSetSamples struct {
	RealSampleRes []RealSamples
}

func FetchAndSampleSurfacesInPointSets(fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObjId, pointSet PointSet) (PointSetSamples, error) {
	logger := slog.Default()
	prefix := "FetchAndSampleSurfacesInPointSets() - "

	numCpusToUse := runtime.GOMAXPROCS(0)
	const decodeToComputeRatio = 10

	numDownloadWorkers := min(20, 2*numCpusToUse)
	numSampleWorkers := max(1, numCpusToUse/(1+decodeToComputeRatio))
	numDecodeWorkers := max(1, numCpusToUse-numSampleWorkers)

	numSurfObjects := len(realSurfObjArr)

	logger.Info(prefix + fmt.Sprintf("sampling %d realization surfaces with workers: download=%d, decode=%d, sample=%d", numSurfObjects, numDownloadWorkers, numDecodeWorkers, numSampleWorkers))

	startTime := time.Now()

	downloadedCh := make(chan *Downloaded, numSurfObjects)
	decodedCh := make(chan *Decoded, numSurfObjects)
	resultCh := make(chan *Result, numSurfObjects)

	runDownloadStage(downloadedCh, resultCh, fetcher, realSurfObjArr, numDownloadWorkers)
	runDecodeStage(downloadedCh, decodedCh, resultCh, numDecodeWorkers)
	runSampleStage(decodedCh, resultCh, pointSet, numSampleWorkers)

	// Compose return array
	totDownloadSizeMb := float32(0)
	realSamplesArr := make([]RealSamples, 0)
	for res := range resultCh {
		logger.Debug(prefix + fmt.Sprintf("realization %d done in %dms, %.2fMB, %.2fMB/s (download=%dms, decode=%dms, sample=%dms)",
			res.realization,
			res.totalDur.Milliseconds(),
			res.downloadSizeMb,
			res.downloadSizeMb/float32(res.totalDur.Seconds()),
			res.downloadDur.Milliseconds(),
			res.decodeDur.Milliseconds(),
			res.sampleDur.Milliseconds(),
		))

		totDownloadSizeMb += res.downloadSizeMb

		if res.err != nil {
			logger.Error("Error processing realization", "realization", res.realization, "err", res.err)
			continue
		}

		realSamples := RealSamples{
			Realization:   res.realization,
			SampledValues: res.sampledValues,
		}

		realSamplesArr = append(realSamplesArr, realSamples)
	}

	totDuration := time.Since(startTime)
	logger.Info(prefix + fmt.Sprintf("Processed %d realizations in %s (total download size: %.2fMB, %.2fM/s)", len(realSamplesArr), totDuration, totDownloadSizeMb, totDownloadSizeMb/(float32(totDuration.Milliseconds())/1000)))

	return PointSetSamples{RealSampleRes: realSamplesArr}, nil
}

type Result struct {
	realization   int
	sampledValues []float32
	err           error
	// Performance metrics
	startTime      time.Time
	totalDur       time.Duration
	downloadDur    time.Duration
	decodeDur      time.Duration
	sampleDur      time.Duration
	downloadSizeMb float32
}

type Downloaded struct {
	realization int
	rawByteData []byte
	res         *Result
}

type Decoded struct {
	realization int
	surface     *xtgeo.Surface
	res         *Result
}

func runDownloadStage(outCh chan<- *Downloaded, resultCh chan<- *Result, fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObjId, workers int) {
	var wg sync.WaitGroup

	numSurfObjects := len(realSurfObjArr)
	tasks := make(chan RealSurfObjId, numSurfObjects)

	for _, realSurfObj := range realSurfObjArr {
		tasks <- realSurfObj
	}
	close(tasks)

	startTime := time.Now()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for realSurfObj := range tasks {
				res := &Result{realization: realSurfObj.Realization, startTime: startTime}

				start := time.Now()
				byteArr, err := fetcher.FetchAsBytes(realSurfObj.ObjectUuid)
				res.downloadDur = time.Since(start)
				res.downloadSizeMb = float32(len(byteArr)) / (1024 * 1024)

				if err != nil {
					res.err = fmt.Errorf("failed to download surface for realization %d (%s): %w", realSurfObj.Realization, realSurfObj.ObjectUuid, err)
					resultCh <- res
					continue
				}

				outCh <- &Downloaded{
					realization: realSurfObj.Realization,
					rawByteData: byteArr,
					res:         res,
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outCh)
	}()
}

func runDecodeStage(inCh <-chan *Downloaded, outCh chan<- *Decoded, resultCh chan<- *Result, workers int) {
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for downloaded := range inCh {
				res := downloaded.res

				start := time.Now()
				surface, err := xtgeo.DeserializeBlobToSurface(downloaded.rawByteData)
				res.decodeDur = time.Since(start)

				if err != nil {
					res.err = fmt.Errorf("failed to decode surface for realization %d: %w", downloaded.realization, err)
					resultCh <- res
					continue
				}

				outCh <- &Decoded{
					realization: downloaded.realization,
					surface:     surface,
					res:         res,
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outCh)
	}()
}

func runSampleStage(inCh <-chan *Decoded, resultCh chan<- *Result, pointSet PointSet, workers int) {
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for decoded := range inCh {
				res := decoded.res
				surface := decoded.surface

				start := time.Now()

				// Sample the surface
				zValueArr, err := xtgeo.SurfaceZArrFromXYPairs(
					pointSet.XCoords, pointSet.YCoords,
					int(surface.Nx), int(surface.Ny),
					surface.Xori, surface.Yori,
					surface.Xinc, surface.Yinc,
					1, surface.Rot,
					surface.DataSlice,
					xtgeo.Bilinear,
				)
				res.sampleDur = time.Since(start)

				if err != nil {
					res.err = fmt.Errorf("failed to sample surface for realization %d: %w", decoded.realization, err)
					resultCh <- res
					continue
				}

				res.totalDur = time.Since(res.startTime)
				res.sampledValues = zValueArr
				resultCh <- res
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}
