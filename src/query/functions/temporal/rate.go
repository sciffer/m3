// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package temporal

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
)

const (
	// IRateType calculates the per-second rate of increase of the time series
	// across the specified time range. This is based on the last two data points.
	IRateType = "irate"

	// IDeltaType calculates the difference between the last two values in the time series.
	// IDeltaTemporalType should only be used with gauges.
	IDeltaType = "idelta"

	// RateType calculates the per-second average rate of increase of the time series.
	RateType = "rate"

	// DeltaType calculates the difference between the first and last value of each time series.
	DeltaType = "delta"

	// IncreaseType calculates the increase in the time series.
	IncreaseType = "increase"
)

// NewRateOp creates a new base temporal transform for rate functions
func NewRateOp(args []interface{}, optype string) (transform.Params, error) {
	if optype == IRateType || optype == IDeltaType || optype == RateType ||
		optype == IncreaseType || optype == DeltaType {
		return newBaseOp(args, optype, newRateNode, nil)
	}

	return nil, fmt.Errorf("unknown rate type: %s", optype)
}

func newRateNode(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	isRate := op.operatorType == IRateType || op.operatorType == RateType
	isCounter := op.operatorType == RateType || op.operatorType == IncreaseType

	var rateFunc rateFunc
	if op.operatorType == IRateType || op.operatorType == IDeltaType {
		rateFunc = irateFunc
	}

	if op.operatorType == RateType || op.operatorType == IncreaseType || op.operatorType == DeltaType {
		rateFunc = normalRate
	}

	return &rateNode{
		op:         op,
		controller: controller,
		timeSpec:   opts.TimeSpec,
		isRate:     isRate,
		isCounter:  isCounter,
		rateFunc:   rateFunc,
	}
}

type rateFunc func([]float64, bool, bool, transform.TimeSpec) float64

type rateNode struct {
	op                baseOp
	controller        *transform.Controller
	timeSpec          transform.TimeSpec
	isRate, isCounter bool
	rateFunc          rateFunc
}

func (r *rateNode) Process(values []float64) float64 {
	return r.rateFunc(values, r.isRate, r.isCounter, r.timeSpec)
}

func normalRate(values []float64, isRate, isCounter bool, timeSpec transform.TimeSpec) float64 {
	var (
		// rangeStart = enh.ts - durationMilliseconds(ms.Range+ms.Offset)
		rangeStart = float64(timeSpec.Start.Unix()) - timeSpec.Step.Seconds()
		rangeEnd   = float64(timeSpec.End.Unix()) - timeSpec.Step.Seconds()
		// rangeEnd   = enh.ts - durationMilliseconds(ms.Offset)
		counterCorrection         float64
		lastValue                 float64
		firstVal                  float64
		firstIdx, lastIdx, numPts int
		foundFirst                bool
	)

	fmt.Println("vals: ", values, rangeStart, rangeEnd)

	if len(values) < 2 {
		return math.NaN()
	}

	for i, val := range values {
		if math.IsNaN(val) {
			continue
		}

		if !foundFirst {
			firstVal = val
			firstIdx = i
		}

		if isCounter && val < lastValue {
			counterCorrection += lastValue
		}
		lastValue = val
		lastIdx = i
		numPts++
	}

	if numPts == 0 {
		return math.NaN()
	}

	resultValue := lastValue - firstVal + counterCorrection
	fmt.Println("res value: ", resultValue, lastValue, lastIdx, firstVal, firstIdx)

	// Duration between first/last samples and boundary of range.
	// use already implemented func to find these
	firstTS := float64(timeSpec.Start.Unix()) + (timeSpec.Step.Seconds() * float64(firstIdx))
	lastTS := float64(timeSpec.Start.Unix()) + (timeSpec.Step.Seconds() * float64(lastIdx))
	durationToStart := float64(firstTS - rangeStart)
	durationToEnd := float64(rangeEnd - lastTS)

	fmt.Println(firstTS, lastTS, durationToStart, durationToStart)

	// sampledInterval := float64(samples.Points[len(samples.Points)-1].T-samples.Points[0].T) / 1000
	sampledInterval := lastTS - firstTS
	averageDurationBetweenSamples := sampledInterval / float64(numPts)

	fmt.Println(sampledInterval, averageDurationBetweenSamples)

	if isCounter && resultValue > 0 && firstVal >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		durationToZero := sampledInterval * (firstVal / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	resultValue = resultValue * (extrapolateToInterval / sampledInterval)
	if isRate {
		resultValue = resultValue / (float64(timeSpec.End.Unix()) - float64(timeSpec.Start.Unix()))
	}

	fmt.Println("res val 2: ", resultValue)

	return resultValue
}

func irateFunc(values []float64, isRate bool, _ bool, timeSpec transform.TimeSpec) float64 {
	valuesLen := len(values)
	if valuesLen < 2 {
		return math.NaN()
	}

	nonNanIdx := valuesLen - 1
	// find idx for last non-NaN value
	indexLast := findNonNanIdx(values, nonNanIdx)
	// if indexLast is 0 then you only have one value and should return a NaN
	if indexLast < 1 {
		return math.NaN()
	}

	nonNanIdx = findNonNanIdx(values, indexLast-1)
	if nonNanIdx == -1 {
		return math.NaN()
	}

	previousSample := values[nonNanIdx]
	lastSample := values[indexLast]

	var resultValue float64
	if isRate && lastSample < previousSample {
		// Counter reset.
		resultValue = lastSample
	} else {
		resultValue = lastSample - previousSample
	}

	if isRate {
		resultValue *= float64(time.Second)
		resultValue /= float64(timeSpec.Step) * float64(indexLast-nonNanIdx)
	}

	return resultValue
}

// findNonNanIdx iterates over the values backwards until we find a non-NaN value,
// then returns its index
func findNonNanIdx(vals []float64, startingIdx int) int {
	for i := startingIdx; i >= 0; i-- {
		if !math.IsNaN(vals[i]) {
			return i
		}
	}

	return -1
}
