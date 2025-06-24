package sample_in_points

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"surface_query/utils"
)

type RealSurfObj struct {
	Realization int
	ObjectUuid  string
}

type PointSet struct {
	XCoords []float64
	YCoords []float64
}

type SamplesForReal struct {
	Realization   int
	SampledValues []float32
}

func FetchAndSampleInPoints(fetcher *utils.BlobFetcher, realSurfObjArr []RealSurfObj, pointSet PointSet) ([]SamplesForReal, error) {
	logger := slog.Default()
	prefix := "sample_in_points - "

	numLogicalCpusAvailable := runtime.GOMAXPROCS(0)
	numDownloadWorkers := max(10, 4*numLogicalCpusAvailable)
	numProcessingWorkers := numLogicalCpusAvailable

	numRealizations := len(realSurfObjArr)

	logger.Info(prefix + fmt.Sprintf("start sampling %d realization surfaces with workers: download=%d, processing=%d", numRealizations, numDownloadWorkers, numProcessingWorkers))

	startTime := time.Now()

	downloadedCh := make(chan *downloadedSurf, numRealizations)
	resultCh := make(chan *pipelineResult, numRealizations)

	runDownloadStage(fetcher, realSurfObjArr, numDownloadWorkers, downloadedCh, resultCh)
	runProcessStage(pointSet, numProcessingWorkers, downloadedCh, resultCh)

	totDownloadSizeMb := float32(0)
	perRealSamples := make([]SamplesForReal, 0, numRealizations)

	for plRes := range resultCh {
		processingDur := plRes.sampleDur + plRes.decodeDur
		logger.Debug(prefix + fmt.Sprintf("realization %d done in %dms, %.2fMB, %.2fMB/s (download=%dms, processing=%dms(%d+%d), queueDownload=%dms, queueProcessing=%dms)",
			plRes.realization,
			plRes.totalDur.Milliseconds(),
			plRes.downloadSizeMb, plRes.downloadSizeMb/float32(plRes.totalDur.Seconds()),
			plRes.downloadDur.Milliseconds(),
			processingDur.Milliseconds(), plRes.decodeDur.Milliseconds(), plRes.sampleDur.Milliseconds(),
			plRes.queueForDownloadDur.Milliseconds(),
			plRes.queueForProcessingDur.Milliseconds(),
		))

		if plRes.err != nil {
			logger.Error("Error processing realization", "realization", plRes.realization, "err", plRes.err)
		}

		totDownloadSizeMb += plRes.downloadSizeMb

		perRealSamples = append(perRealSamples, SamplesForReal{
			Realization:   plRes.realization,
			SampledValues: plRes.sampledValues,
		})
	}

	totDuration := time.Since(startTime)
	logger.Info(prefix + fmt.Sprintf("finished processing %d realizations surfaces in %.2fs (download totals: %.2fMB, %.2fMB/s)", numRealizations, totDuration.Seconds(), totDownloadSizeMb, totDownloadSizeMb/(float32(totDuration.Milliseconds())/1000)))

	return perRealSamples, nil
}
