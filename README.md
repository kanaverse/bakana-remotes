# Remote readers for bakana

![Unit tests](https://github.com/kanaverse/bakana-remotes/actions/workflows/run-tests.yaml/badge.svg)
![Documentation](https://github.com/kanaverse/bakana-remotes/actions/workflows/build-docs.yaml/badge.svg)
![NPM](https://img.shields.io/npm/v/bakana-remotes)

## Overview

This package provides a variety of remote readers for the [**bakana**](https://npmjs.org/package/bakana) package,
allowing us to perform scRNA-seq analyses on datasets from other sources.
Currently we support:

- [**takane**](https://github.com/ArtifactDB/takane)-formatted -`SummarizedExperiment` objects in the [**gypsum**](https://github.com/ArtifactDB/gypsum-worker) bucket.
- [**takane**](https://github.com/ArtifactDB/takane)-formatted -`SummarizedExperiment` objects via the [**SewerRat**](https://github.com/ArtifactDB/gypsum-worker) API.

## Quick start

Usage is as simple as:

```js
import * as remotes from "bakana-remotes";
import * as bakana from "bakana";

// Add desired readers for unserialization.
bakana.availableReaders["gypsum"] = remotes.GypsumDataset;
```

For **gypsum**-sourced datasets, we can construct the following object, which uses the Zeisel brain single-cell dataset from 2015:

```js
let gyp = new remotes.GypsumDataset("scRNAseq", "zeisel-brain-2015", "2023-12-14");
```

Similarly, **SewerRat**-sourced datasets can be pulled from shared filesystems (e.g., on HPCs) using the following:

```js
let sewer = new remotes.SewerRatDataset(
    "/path/to/dataset", 
    url = "https://somewhere.edu/sewerrat"
);
```

These objects can then be used in an entry of `datasets` in [`bakana.runAnalysis()`](https://ltla.github.io/bakana/global.html#runAnalysis).

We also provide the equivalent `Result` objects, if users just want to read existing analysis results:

```js
let gyp2 = new remotes.GypsumResult(
    "scRNAseq", 
    "ernst-spermatogenesis-2019", 
    "2023-12-21",
    path = "emptydrop"
);

let sewer2 = new remotes.SewerRatResult(
    "/path/to/result", 
    url = "https://somewhere.edu/sewerrat"
);
```

Check out the [API documentation](https://kanaverse.github.io/bakana-remotes) for more details. 

## Links

See the [**bakana**](https://github.com/kanaverse/bakana) documentation for more details on how to create a 
[custom `Dataset` reader](https://github.com/kanaverse/bakana/blob/master/docs/related/custom_readers.md).
Implementations of readers for other databases are welcome.
