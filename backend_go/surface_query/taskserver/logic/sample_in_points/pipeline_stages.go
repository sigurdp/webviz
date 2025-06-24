package sample_in_points

import (
	"fmt"
	"sync"
	"time"

	"surface_query/utils"
	"surface_query/xtgeo"
)

type pipelineResult struct {
	realization   int
	sampledValues []float32
	err           error
	// Performance metrics
	downloadDur           time.Duration
	decodeDur             time.Duration
	sampleDur             time.Duration
	queueForDownloadDur   time.Duration
	queueForProcessingDur time.Duration
	downloadStartTime     time.Time
	downloadSizeMb        float32
	totalDur              time.Duration // Total duration from from start of download to end of sampling
}

type downloadedSurf struct {
	realization int
	rawByteData []byte
	res         *pipelineResult
	enqueuedAt  time.Time
}

func runDownloadStage(fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObj, numWorkers int, outCh chan<- *downloadedSurf, resultCh chan<- *pipelineResult) {
	var wg sync.WaitGroup

	stageStartTime := time.Now()

	numSurfObjects := len(realSurfObjArr)
	tasks := make(chan RealSurfObj, numSurfObjects)

	for _, realSurfObj := range realSurfObjArr {
		tasks <- realSurfObj
	}
	close(tasks)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for realSurfObj := range tasks {
				downloadStartTime := time.Now()

				res := &pipelineResult{
					realization:         realSurfObj.Realization,
					downloadStartTime:   downloadStartTime,
					queueForDownloadDur: time.Since(stageStartTime),
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

func runProcessStage(pointSet PointSet, numWorkers int, inCh <-chan *downloadedSurf, resultCh chan<- *pipelineResult) {
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for downloaded := range inCh {
				res := downloaded.res

				startDecode := time.Now()
				res.queueForProcessingDur = startDecode.Sub(downloaded.enqueuedAt)

				surface, err := xtgeo.DeserializeBlobToSurface(downloaded.rawByteData)
				res.decodeDur = time.Since(startDecode)

				if err != nil {
					res.err = fmt.Errorf("failed to decode surface for realization %d: %w", downloaded.realization, err)
					resultCh <- res
					continue
				}

				startSample := time.Now()

				res.sampledValues, _ = xtgeo.SurfaceZArrFromXYPairs(
					pointSet.XCoords, pointSet.YCoords,
					int(surface.Nx), int(surface.Ny),
					surface.Xori, surface.Yori,
					surface.Xinc, surface.Yinc,
					1, surface.Rot,
					surface.DataSlice,
					xtgeo.Bilinear,
				)

				res.sampleDur = time.Since(startSample)

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
