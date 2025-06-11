package operations

/*
import (
	"log/slog"
	"surface_query/utils"
	"surface_query/xtgeo"
	"sync"
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

func FetchAndSampleSurfacesInPointSets(fetcher *utils.BlobFetcher, realSurfObjIdArr []RealSurfObjId, pointSet PointSet) (PointSetSamples, error) {
	logger := slog.Default()

	numObjectIds := len(realSurfObjIdArr)

	// Pre-allocate the working arrays with the known length so we can access them without locking
	// type ResultItem struct {
	// 	sampledValues []float32
	// }
	// resultItemArr := make([]*ResultItem, numObjectIds)
	// errorArray := make([]error, numObjectIds)

	const maxDownloaders = 50
	const maxDeserializers = 4
	const maxProcessors = 4

	type RawSurfData struct {
		realization int
		rawBytes    []byte
		err         error
	}

	type LoadedSurfData struct {
		realization int
		surface     *xtgeo.Surface
		err         error
	}

	type ResultData struct {
		realization int
		valueArr    []float32
		err         error
	}

	downloadSem := make(chan struct{}, maxDownloaders)
	toDeserializeChan := make(chan RawSurfData, numObjectIds)
	toProcessChan := make(chan LoadedSurfData, numObjectIds)
	resultsChan := make(chan ResultData, numObjectIds)

	// Download phase
	var downloadWg sync.WaitGroup
	for _, realObjId := range realSurfObjIdArr {
		downloadWg.Add(1)

		go func(realObjId RealSurfObjId) {
			defer downloadWg.Done()

			downloadSem <- struct{}{}
			byteArr, err := fetcher.FetchAsBytes(realObjId.ObjectUuid)
			<-downloadSem

			toDeserializeChan <- RawSurfData{
				realization: realObjId.Realization,
				rawBytes:    byteArr,
				err:         err,
			}
		}(realObjId)
	}

	// Deserialize/parse xtgeo
	var deserializeWg sync.WaitGroup
	for i := 0; i < maxDeserializers; i++ {
		deserializeWg.Add(1)

		go func() {
			defer deserializeWg.Done()

			for rawSurfData := range toDeserializeChan {
				logger.Debug("Deserializing real: ", "realization", rawSurfData.realization)
				if rawSurfData.err != nil {
					toProcessChan <- LoadedSurfData{realization: rawSurfData.realization, surface: nil, err: rawSurfData.err}
					continue
				}

				surface, err := xtgeo.DeserializeBlobToSurface(rawSurfData.rawBytes)
				if err != nil {
					logger.Error("Error decoding blob as surface:", slog.Any("error", err))
					toProcessChan <- LoadedSurfData{realization: rawSurfData.realization, surface: nil, err: err}
					continue
				}

				toProcessChan <- LoadedSurfData{
					realization: rawSurfData.realization,
					surface:     surface,
					err:         nil,
				}
			}
		}()
	}

	// Processing workers
	var processWg sync.WaitGroup
	for i := 0; i < maxProcessors; i++ {
		processWg.Add(1)

		go func() {
			defer processWg.Done()

			for loadedSurfData := range toProcessChan {
				logger.Debug("Processing real: ", "realization", loadedSurfData.realization)
				if loadedSurfData.err != nil {
					resultsChan <- ResultData{realization: loadedSurfData.realization, valueArr: nil, err: loadedSurfData.err}
					continue
				}

				surface := loadedSurfData.surface

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

				if err != nil {
					resultsChan <- ResultData{realization: loadedSurfData.realization, valueArr: nil, err: err}
					continue
				}

				resultsChan <- ResultData{
					realization: loadedSurfData.realization,
					valueArr:    zValueArr,
					err:         nil,
				}
			}
		}()
	}

	go func() {
		downloadWg.Wait()
		close(toDeserializeChan)
	}()

	go func() {
		deserializeWg.Wait()
		close(toProcessChan)
	}()

	go func() {
		processWg.Wait()
		close(resultsChan)
	}()

	// Construct return array
	realSampleResArr := make([]RealSamples, 0)
	for resultData := range resultsChan {
		logger.Debug("Construct result for realization: ", "realization", resultData.realization, "length", len(resultData.valueArr), "error", resultData.err)
		if resultData.err == nil {
			realResult := RealSamples{
				Realization:   resultData.realization,
				SampledValues: resultData.valueArr,
			}
			realSampleResArr = append(realSampleResArr, realResult)
		}
		logger.Debug("length of realSampleResArr: ", "length", len(realSampleResArr))
	}

	return PointSetSamples{RealSampleRes: realSampleResArr}, nil
}
*/
