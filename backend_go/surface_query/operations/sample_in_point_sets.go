package operations

import (
	"fmt"
	"log/slog"
	"math"
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
	const computeToDecodeRatio float64 = 0.1

	numDownloadWorkers := max(10, 4*numCpusToUse)
	numDecodeWorkers := max(1, int(math.Round(float64(numCpusToUse)*(1.0-computeToDecodeRatio))))
	numSampleWorkers := max(1, numCpusToUse-numDecodeWorkers)

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
		logger.Debug(prefix + fmt.Sprintf("realization %d done in %dms, %.2fMB, %.2fMB/s (download=%dms(q=%dms), decode=%dms(q=%dms), sample=%dms(q=%dms))",
			res.realization,
			res.totalDur.Milliseconds(),
			res.downloadSizeMb,
			res.downloadSizeMb/float32(res.totalDur.Seconds()),
			res.downloadDur.Milliseconds(), res.queueDurDownload.Milliseconds(),
			res.decodeDur.Milliseconds(), res.queueDurDecode.Milliseconds(),
			res.sampleDur.Milliseconds(), res.queueDurSample.Milliseconds(),
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
	logger.Info(prefix + fmt.Sprintf("Processed %d realizations in %s (download totals: %.2fMB, %.2fMB/s)", len(realSamplesArr), totDuration, totDownloadSizeMb, totDownloadSizeMb/(float32(totDuration.Milliseconds())/1000)))

	return PointSetSamples{RealSampleRes: realSamplesArr}, nil
}

type Result struct {
	realization   int
	sampledValues []float32
	err           error
	// Performance metrics
	downloadDur       time.Duration
	decodeDur         time.Duration
	sampleDur         time.Duration
	queueDurDownload  time.Duration
	queueDurDecode    time.Duration
	queueDurSample    time.Duration
	downloadStartTime time.Time
	totalDur          time.Duration // Total duration from from start of download to end of sampling
	downloadSizeMb    float32
}

type Downloaded struct {
	realization int
	rawByteData []byte
	res         *Result
	enqueuedAt  time.Time
}

type Decoded struct {
	realization int
	surface     *xtgeo.Surface
	res         *Result
	enqueuedAt  time.Time
}

func runDownloadStage(outCh chan<- *Downloaded, resultCh chan<- *Result, fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObjId, workers int) {
	var wg sync.WaitGroup

	stageStartTime := time.Now()

	numSurfObjects := len(realSurfObjArr)
	tasks := make(chan RealSurfObjId, numSurfObjects)

	for _, realSurfObj := range realSurfObjArr {
		tasks <- realSurfObj
	}
	close(tasks)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for realSurfObj := range tasks {
				downloadStartTime := time.Now()

				res := &Result{
					realization:       realSurfObj.Realization,
					downloadStartTime: downloadStartTime,
					queueDurDownload:  time.Since(stageStartTime),
				}

				byteArr, err := fetcher.FetchAsBytes(realSurfObj.ObjectUuid)
				res.downloadDur = time.Since(downloadStartTime)
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
					enqueuedAt:  time.Now(),
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
				res.queueDurDecode = start.Sub(downloaded.enqueuedAt)

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
					enqueuedAt:  time.Now(),
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
				res.queueDurSample = start.Sub(decoded.enqueuedAt)

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

				res.totalDur = time.Since(res.downloadStartTime)
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
