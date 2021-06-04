# TopK: "filtered space saving" streaming topk algorithm

A modified version of http://github.com/dgryski/go-topk with the following changes

* [x] Add msgp encoding/decoding
* [x] Use metro hash
* [x] Allow merging via https://ieeexplore.ieee.org/document/8438445
