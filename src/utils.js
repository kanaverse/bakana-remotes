import * as bioc from "bioconductor";

export var downloadFun = async url => {
    let resp = await fetch(url);
    if (!resp.ok) {
        throw new Error("failed to fetch content at " + url + "(" + resp.status + ")");
    }
    return new Uint8Array(await resp.arrayBuffer());
};

/**
 * Specify a function to download remote resources.
 *
 * @param {function} fun - Function that accepts a single string containing a URL,
 * and returns a Uint8Array of that URL's contents.
 *
 * @return `fun` is set as the global downloader for this step.
 * The _previous_ value of the downloader is returned.
 */
export function setDownloadFun(fun) {
    let previous = downloadFun;
    downloadFun = fun;
    return previous;
}

export const defaultModality = "";

export function createSoloDefaultModality(value) {
    let output = {};
    output[defaultModality] = value;
    return output;
}

export function cloneCached(x, cached) {
    return (cached ? bioc.CLONE(x) : x);
}

/**
 * A representation of a matrix of expression values, where the values are hosted on the Wasm heap for easier compute via [**scran.js**](https://github.com/jkanche/scran.js).
 * See [here](https://jkanche.github.io/scran.js/ScranMatrix.html) for more details.
 *
 * @external ScranMatrix
 */ 

/**
 * A representation of multiple {@linkplain external:ScranMatrix ScranMatrix} objects, where each object contains data for the same cells but across a different feature space, e.g., for different data modalities.
 * See [here](https://jkanche.github.io/scran.js/MultiMatrix.html) for more details.
 *
 * @external MultiMatrix
 */ 


/**
 * A DataFrame from the [**bioconductor**](https://github.com/LTLA/bioconductor.js) package, where each column is represented by some arbitrary vector-like object.
 * See [here](https://ltla.github.io/bioconductor.js/DataFrame.html) for more details.
 *
 * @external DataFrame
 */ 

/**
 * Representation of a file that is agnostic to the environment (Node.js or browser) or the nature of the contents (buffer or file path).
 * See [here](https://ltla.github.io/bakana/SimpleFile.html) for more details.
 *
 * @external SimpleFile
 */ 
