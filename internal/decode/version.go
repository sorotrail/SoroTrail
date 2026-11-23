package decode

// CurrentDecoderVersion is the hand-maintained version number of the decoder pipeline.
// It is bumped deliberately whenever decoder logic changes so replay can target events
// that were decoded with older versions.
const CurrentDecoderVersion = 1
