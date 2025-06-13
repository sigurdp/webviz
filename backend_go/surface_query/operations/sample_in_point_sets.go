package operations

import (
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"sync"
	"time"

	"surface_query/utils"
	"surface_query/xtgeo"
)

type RealSurfObj struct {
	Realization int
	ObjectUuid  string
}

type PointSet struct {
	Name    string
	XCoords []float64
	YCoords []float64
}

type SamplesForReal struct {
	Realization   int
	SampledValues []float32
}

type PointSetResult struct {
	PointSetName   string
	PerRealSamples []SamplesForReal
}

func FetchAndSampleSurfacesInPointSets(fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObj, pointSetArr []PointSet) ([]PointSetResult, error) {
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

	downloadedCh := make(chan *downloadedSurf, numSurfObjects)
	decodedCh := make(chan *decodedSurf, numSurfObjects)
	resultCh := make(chan *pipelineResult, numSurfObjects)

	runDownloadStage(downloadedCh, resultCh, fetcher, realSurfObjArr, numDownloadWorkers)
	runDecodeStage(downloadedCh, decodedCh, resultCh, numDecodeWorkers)
	runSampleStage(decodedCh, resultCh, pointSetArr, numSampleWorkers)

	// Compose the return array
	// We need to transform the pipeline results since the output of the pipeline is per realization surface
	// while the return array collects results per point set
	totDownloadSizeMb := float32(0)

	numPointSets := len(pointSetArr)
	pointSetResultArr := make([]PointSetResult, numPointSets)
	for ips := 0; ips < numPointSets; ips++ {
		pointSetResultArr[ips].PointSetName = pointSetArr[ips].Name
	}

	for pRes := range resultCh {
		logger.Debug(prefix + fmt.Sprintf("realization %d done in %dms, %.2fMB, %.2fMB/s (download=%dms(q=%dms), decode=%dms(q=%dms), sample=%dms(q=%dms))",
			pRes.realization,
			pRes.totalDur.Milliseconds(),
			pRes.downloadSizeMb,
			pRes.downloadSizeMb/float32(pRes.totalDur.Seconds()),
			pRes.downloadDur.Milliseconds(), pRes.queueDurDownload.Milliseconds(),
			pRes.decodeDur.Milliseconds(), pRes.queueDurDecode.Milliseconds(),
			pRes.sampleDur.Milliseconds(), pRes.queueDurSample.Milliseconds(),
		))

		totDownloadSizeMb += pRes.downloadSizeMb

		if pRes.err != nil {
			logger.Error("Error processing realization", "realization", pRes.realization, "err", pRes.err)
			continue
		}

		if len(pRes.perPointSetSamples) != numPointSets {
			return nil, fmt.Errorf("expected %d point sets, got %d for realization %d", numPointSets, len(pRes.perPointSetSamples), pRes.realization)
		}

		for ips := 0; ips < numPointSets; ips++ {
			samplesInThisReal := SamplesForReal{
				Realization:   pRes.realization,
				SampledValues: pRes.perPointSetSamples[ips].sampledValues,
			}

			pointSetResultArr[ips].PerRealSamples = append(pointSetResultArr[ips].PerRealSamples, samplesInThisReal)
		}
	}

	totDuration := time.Since(startTime)
	logger.Info(prefix + fmt.Sprintf("Processed %d realizations in %s (download totals: %.2fMB, %.2fMB/s)", len(realSurfObjArr), totDuration, totDownloadSizeMb, totDownloadSizeMb/(float32(totDuration.Milliseconds())/1000)))

	return pointSetResultArr, nil
}

type singleRealPointSetSamples struct {
	pointSetName  string    // same as in PointSet
	sampledValues []float32 // values sampled from surface at each (x, y) in the input PointSet
}

type pipelineResult struct {
	realization        int
	perPointSetSamples []singleRealPointSetSamples // one entry for each of the input PointSets
	err                error
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

type downloadedSurf struct {
	realization int
	rawByteData []byte
	res         *pipelineResult
	enqueuedAt  time.Time
}

type decodedSurf struct {
	realization int
	surface     *xtgeo.Surface
	res         *pipelineResult
	enqueuedAt  time.Time
}

func runDownloadStage(outCh chan<- *downloadedSurf, resultCh chan<- *pipelineResult, fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObj, workers int) {
	var wg sync.WaitGroup

	stageStartTime := time.Now()

	numSurfObjects := len(realSurfObjArr)
	tasks := make(chan RealSurfObj, numSurfObjects)

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

				res := &pipelineResult{
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

				outCh <- &downloadedSurf{
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

func runDecodeStage(inCh <-chan *downloadedSurf, outCh chan<- *decodedSurf, resultCh chan<- *pipelineResult, workers int) {
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

				outCh <- &decodedSurf{
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

func runSampleStage(inCh <-chan *decodedSurf, resultCh chan<- *pipelineResult, pointSetArr []PointSet, workers int) {
	numPointSets := len(pointSetArr)

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

				// Loop over all the point sets and sample the surface
				res.perPointSetSamples = make([]singleRealPointSetSamples, numPointSets)

				for i := 0; i < numPointSets; i++ {
					pointSet := pointSetArr[i]
					valueArr, _ := xtgeo.SurfaceZArrFromXYPairs(
						pointSet.XCoords, pointSet.YCoords,
						int(surface.Nx), int(surface.Ny),
						surface.Xori, surface.Yori,
						surface.Xinc, surface.Yinc,
						1, surface.Rot,
						surface.DataSlice,
						xtgeo.Bilinear,
					)

					res.perPointSetSamples[i].pointSetName = pointSet.Name
					res.perPointSetSamples[i].sampledValues = valueArr
				}
				res.sampleDur = time.Since(start)

				res.totalDur = time.Since(res.downloadStartTime)
				resultCh <- res
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}
