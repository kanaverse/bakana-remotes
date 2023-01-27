import * as scran from "scran.js";
import * as bioc from "bioconductor";
import * as bakana from "bakana";
import * as adb from "artifactdb";
import * as utils from "./utils.js";
import * as adb_utils from "./ArtifactDB.js";

const baseUrl = "https://collaboratordb.aaron-lun.workers.dev";

/**
 * Dataset derived from a SummarizedExperiment in [CollaboratorDB](https://github.com/CollaboratorDB).
 */
export class CollaboratorDBDataset {
    #id;
    #unpacked;

    #metadata;
    #alternatives;

    #rowdata;
    #coldata;

    #countAssayRnaName;
    #countAssayAdtName;
    #countAssayCrisprName;

    #experimentRnaName;
    #experimentAdtName;
    #experimentCrisprName;

    #primaryRnaFeatureIdColumn;
    #primaryAdtFeatureIdColumn;
    #primaryCrisprFeatureIdColumn;

    #dump_options() {
        return {
            countAssayRnaName: this.#countAssayRnaName,
            countAssayAdtName: this.#countAssayAdtName,
            countAssayCrisprName: this.#countAssayCrisprName,
            experimentRnaName: this.#experimentRnaName,
            experimentAdtName: this.#experimentAdtName,
            experimentCrisprName: this.#experimentCrisprName,
            primaryRnaFeatureIdColumn: this.#primaryRnaFeatureIdColumn,
            primaryAdtFeatureIdColumn: this.#primaryAdtFeatureIdColumn,
            primaryCrisprFeatureIdColumn: this.#primaryCrisprFeatureIdColumn
        };
    }

    /****************************************
     ****************************************/

    static #getFun = null;
    static #downloadFun = null;

    /** 
     * @param {?function} fun - Function that accepts a URL string and downloads the resource,
     * see [`getFileMetadata`](https://artifactdb.github.io/artifactdb.js/global.html#getFileMetadata) for details.
     * 
     * Alternatively `null`, to reset the function to its default value.
     * @return {?function} Previous setting of the download function.
     */
    static setDownloadFun(fun) {
        let previous = CollaboratorDBDataset.#downloadFun;
        CollaboratorDBDataset.#downloadFun = fun;
        return previous;
    }

    /** 
     * @param {?function} fun - Function that accepts a URL string and performs a GET to return a Response object,
     * see [`getFileMetadata`](https://artifactdb.github.io/artifactdb.js/global.html#getFileMetadata) for details.
     * 
     * Alternatively `null`, to reset the function to its default value.
     * @return {?function} Previous setting of the GET function.
     */
    static setGetFun(fun) {
        let previous = CollaboratorDBDataset.#getFun;
        CollaboratorDBDataset.#getFun = fun;
        return previous;
    }

    /****************************************
     ****************************************/

    /**
     * @param {string} id - Identifier of a SummarizedExperiment in CollaboratorDB.
     * @param {object} [options={}] - Optional parameters.
     * @param {?string} [options.countAssayRnaName=null] - See {@linkcode CollaboratorDBDataset#setCountAssayRnaName setCountAssayRnaName}.
     * @param {?string} [options.countAssayAdtName=null] - See {@linkcode CollaboratorDBDataset#setCountAssayAdtName setCountAssayAdtName}.
     * @param {?string} [options.countAssayCrisprName=null] - See {@linkcode CollaboratorDBDataset#setCountAssayCrisprName setCountAssayCrisprName}.
     * @param {?string} [options.experimentRnaName=""] - See {@linkcode CollaboratorDBDataset#setExperimentRnaName setExperimentRnaName}.
     * @param {?string} [options.experimentAdtName=null] - See {@linkcode CollaboratorDBDataset#setExperimentAdtName setExperimentAdtName}.
     * @param {?string} [options.experimentCrisprName=null] - See {@linkcode CollaboratorDBDataset#setExperimentCrisprName setExperimentCrisprName}.
     * @param {string|number} [options.primaryRnaFeatureIdColumn=0] - See {@linkcode CollaboratorDBDataset#setPrimaryRnaFeatureIdColumn setPrimaryRnaFeatureIdColumn}.
     * @param {string|number} [options.primaryAdtFeatureIdColumn=0] - See {@linkcode CollaboratorDBDataset#setPrimaryAdtFeatureIdColumn setPrimaryAdtFeatureIdColumn}.
     * @param {string|number} [options.primaryCrisprFeatureIdColumn=0] - See {@linkcode CollaboratorDBDataset#setPrimaryCrisprFeatureIdColumn setPrimaryCrisprFeatureIdColumn}.
     */
    constructor(id, { 
        countAssayRnaName = null, 
        countAssayAdtName = null, 
        countAssayCrisprName = null, 
        experimentRnaName = "",
        experimentAdtName = null,
        experimentCrisprName = null,
        primaryRnaFeatureIdColumn = 0, 
        primaryAdtFeatureIdColumn = 0,
        primaryCrisprFeatureIdColumn = 0
    } = {}) {

        this.#id = id;
        this.#unpacked = adb.unpackId(id);

        this.#countAssayRnaName = countAssayRnaName;
        this.#countAssayAdtName = countAssayAdtName;
        this.#countAssayCrisprName = countAssayCrisprName;

        this.#experimentRnaName = experimentRnaName;
        this.#experimentAdtName = experimentAdtName;
        this.#experimentCrisprName = experimentCrisprName;

        this.#primaryRnaFeatureIdColumn = primaryRnaFeatureIdColumn;
        this.#primaryAdtFeatureIdColumn = primaryAdtFeatureIdColumn;
        this.#primaryCrisprFeatureIdColumn = primaryCrisprFeatureIdColumn;

        this.#metadata = null;
        this.clear();
    }

    /**
     * @return {string} Format of this dataset class.
     * @static
     */
    static format() {
        return "CollaboratorDB";
    }

    /**
     * Destroy caches if present, releasing the associated memory.
     * This may be called at any time but only has an effect if `cache = true` in {@linkcode CollaboratorDBDataset#load load} or {@linkcodeCollaboratorDBDataset#annotations annotations}. 
     */
    clear() {
        this.#rowdata = null;
        this.#coldata = null;
    }

    /**
     * @return {object} Object containing the abbreviated details of this dataset.
     */
    abbreviate() {
        return { "id": this.#id, "options": this.#dump_options() };
    }

    /****************************************
     ****************************************/

    /**
     * @param {?string} name - Name of the assay containing the count matrix for the RNA experiment.
     * If `null`, the first encountered assay is used.
     */
    setCountAssayRnaName(name) {
        this.#countAssayRnaName = name;
        return;
    }

    /**
     * @param {?string} name - Name of the assay containing the count matrix for the ADT experiment.
     * If `null`, the first encountered assay is used.
     */
    setCountAssayAdtName(name) {
        this.#countAssayAdtName = name;
        return;
    }

    /**
     * @param {?string} name - Name of the assay containing the count matrix for the CRISPR experiment.
     * If `null`, the first encountered assay is used.
     */
    setCountAssayCrisprName(name) {
        this.#countAssayCrisprName = name;
        return;
    }

    /**
     * @param {?string} name - Name of the experiment for gene expression.
     * Alternatively `null`, to indicate that no RNA features are to be loaded.
     */
    setExperimentRnaName(name) {
        this.#experimentRnaName = name;
        return;
    }

    /**
     * @param {string} name - Name of the experiment for ADTs.
     * Alternatively `null`, to indicate that no ADT features are to be loaded.
     */
    setExperimentAdtName(name) {
        this.#experimentAdtName = name;
        return;
    }

    /**
     * @param {?string} name - Name of the experiment for CRISPR guides.
     * Alternatively `null`, to indicate that no guides are to be loaded.
     */
    setExperimentCrisprName(name) {
        this.#experimentCrisprName = name;
        return;
    }

    /**
     * @param {string|number} i - Name or index of the column of the `features` {@linkplain external:DataFrame DataFrame} that contains the primary feature identifier for gene expression.
     * If `i` is invalid (e.g., out of range index, unavailable name), it is ignored and the primary identifier is treated as undefined.
     */
    setPrimaryRnaFeatureIdColumn(i) {
        this.#primaryRnaFeatureIdColumn = i;
        return;
    }

    /**
     * @param {?(string|number)} i - Name or index of the column of the `features` {@linkplain external:DataFrame DataFrame} that contains the primary feature identifier for the ADTs.
     * If `i` is invalid (e.g., out of range index, unavailable name), it is ignored and the primary identifier is treated as undefined.
     */
    setPrimaryAdtFeatureIdColumn(i) {
        this.#primaryAdtFeatureIdColumn = i;
        return;
    }

    /**
     * @param {?(string|number)} i - Name or index of the column of the `features` {@linkplain external:DataFrame DataFrame} that contains the primary feature identifier for the CRISPR guides.
     * If `i` is invalid (e.g., out of range index, unavailable name), it is ignored and the primary identifier is treated as undefined.
     */
    setPrimaryCrisprFeatureIdColumn(i) {
        this.#primaryCrisprFeatureIdColumn = i;
        return;
    }

    /****************************************
     ****************************************/

    async #core() {
        if (this.#metadata !== null) {
            return;
        }

        let meta = await adb.getFileMetadata(baseUrl, this.#id, { getFun: this.constructor.#getFun });
        this.#metadata = meta;
        if (!("summarized_experiment" in meta)) {
            throw new Error("no 'summarized_experiment' property present in the metadata");
        }

        this.#alternatives = {};
        if ("single_cell_experiment" in meta) {
            let scmeta = meta.single_cell_experiment;
            if ("alternative_experiments" in scmeta) {
                let collected = [];
                let names = [];
                for (const x of scmeta.alternative_experiments) {
                    names.push(x.name);
                    let alt_id = adb.packId(this.#unpacked.project, x.resource.path, this.#unpacked.version);
                    collected.push(adb.getFileMetadata(baseUrl, alt_id, { getFun: this.constructor.#getFun }));
                }

                let resolved = await Promise.all(collected);
                for (var i = 0; i < resolved.length; i++) {
                    this.#alternatives[names[i]] = resolved[i];
                }
            }
        }
    }

    async #cells() {
        if (this.#coldata !== null) {
            return;
        }

        await this.#core();
        let meta = this.#metadata;
        let smeta = meta.summarized_experiment;

        if ("column_data" in smeta) {
            let df_id = adb.packId(this.#unpacked.project, smeta.column_data.resource.path, this.#unpacked.version);
            this.#coldata = await adb_utils.loadDataFrame(baseUrl, df_id, { getFun: this.constructor.#getFun, downloadFun: this.constructor.#downloadFun });
        } else {
            this.#coldata = new bioc.DataFrame({}, { numberOfRows: smeta.dimensions[1] });
        }

        return;
    }

    derive_default_name(meta) {
        return "";
    }

    async #features() {
        if (this.#coldata !== null) {
            return;
        }

        let tasks = [];
        let names = [];
        let loader = (se_meta, se_name) => {
            names.push(se_name);
            if ("row_data" in se_meta) {
                let df_id = adb.packId(this.#unpacked.project, se_meta.row_data.resource.path, this.#unpacked.version);
                tasks.push(adb_utils.loadDataFrame(baseUrl, df_id, { getFun: this.constructor.#getFun, downloadFun: this.constructor.#downloadFun }));
            } else {
                tasks.push(new bioc.DataFrame({}, { numberOfRows: se_meta.summarized_experiment.dimensions[0] }));
            }
        };

        await this.#core();
        let meta = this.#metadata;
        loader(meta.summarized_experiment, this.derive_default_name(meta));
        for (const [k, v] of Object.entries(this.#alternatives)) {
            loader(v.summarized_experiment, k);
        }

        let output = {};
        let resolved = await Promise.all(tasks);
        for (var i = 0; i < resolved.length; i++) {
            output[names[i]] = resolved[i];
        }
        this.#rowdata = output;
        return;
    }

    /**
     * @param {object} [options={}] - Optional parameters.
     * @param {boolean} [options.cache=false] - Whether to cache the results for re-use in subsequent calls to this method or {@linkcode CollaboratorDBDataset#load load}.
     * If `true`, users should consider calling {@linkcode CollaboratorDBDataset#clear clear} to release the memory once this dataset instance is no longer needed.
     * 
     * @return {object} Object containing the per-feature and per-cell annotations.
     * This has the following properties:
     *
     * - `modality_features`: an object where each key is a modality name and each value is a {@linkplain external:DataFrame DataFrame} of per-feature annotations for that modality.
     *   Unlike {@linkcode CollaboratorDBDataset#load load}, modality names are arbitrary.
     * - `modality_assay_names`: an object where each key is a modality name and each value is an Array of assay names for that modality.
     *   This should match the modality names in `modality_features`.
     * - `cells`: a {@linkplain external:DataFrame DataFrame} of per-cell annotations.
     */
    async summary({ cache = false } = {}) {
        await this.#features();
        await this.#cells();

        let output = { 
            cells: utils.cloneCached(this.#coldata, cache),
            modality_features: utils.cloneCached(this.#rowdata, cache)
        };

        // Fetching the assay names for each modality.
        let modality_assay_names = {};
        let loader = (se_meta, se_name) => {
            let assmeta = se_meta.assays;
            let current = [];
            for (var i = 0; i < assmeta.length; i++) {
                current.push(assmeta[i].name);
            }
            modality_assay_names[se_name] = current;
        };

        await this.#core();
        let meta = this.#metadata;
        loader(meta.summarized_experiment, this.derive_default_name(meta));
        for (const [k, v] of Object.entries(this.#alternatives)) {
            loader(v.summarized_experiment, k);
        }
        output.modality_assay_names = modality_assay_names;

        if (!cache) {
            this.clear();
        }
        return output;
    }

    /****************************************
     ****************************************/

    /**
     * @param {object} [options={}] - Optional parameters.
     * @param {boolean} [options.cache=false] - Whether to cache the results for re-use in subsequent calls to this method or {@linkcode TenxHdf5Dataset#summary summary}.
     * If `true`, users should consider calling {@linkcode TenxHdf5Dataset#clear clear} to release the memory once this dataset instance is no longer needed.
     *
     * @return {object} Object containing the per-feature and per-cell annotations.
     * This has the following properties:
     *
     * - `features`: an object where each key is a modality name and each value is a {@linkplain external:DataFrame DataFrame} of per-feature annotations for that modality.
     * - `cells`: a {@linkplain external:DataFrame DataFrame} containing per-cell annotations.
     * - `matrix`: a {@linkplain external:MultiMatrix MultiMatrix} containing one {@linkplain external:ScranMatrix ScranMatrix} per modality.
     * - `row_ids`: an object where each key is a modality name and each value is an integer array containing the feature identifiers for each row in that modality.
     *
     * Modality names are guaranteed to be one of `"RNA"`, `"ADT"` or `"CRISPR"`.
     * It is assumed that an appropriate mapping from the feature types inside the `featureFile` was previously declared,
     * either in the constructor or in setters like {@linkcode setExperimentRnaName}.
     */
    async load({ cache = false } = {}) {
        await this.#features();
        await this.#cells();

        await this.#core();
        let meta = this.#metadata;

        // Collating metadata for ensuing lookup.
        let all_meta = {};
        all_meta[this.derive_default_name(meta)] = meta;
        for (const [k, v] of Object.entries(this.#alternatives)) {
            all_meta[k] = v;
        }

        // Rolling through all modalities.
        let out_mat = new scran.MultiMatrix;
        let out_rids = {};
        let out_feats = {};
        let tasks = [];

        let loader = (modality_name, exp_name, assay_name) => {
            if (exp_name === null || !(exp_name in all_meta)) {
                return;
            }

            let se_meta = all_meta[exp_name];
            let assmeta = se_meta.summarized_experiment.assays;

            let chosen = null;
            if (assay_name !== null) {
                for (var i = 0; i < assmeta.length; i++) {
                    if (assmeta[i].name === assay_name) {
                        chosen = i;
                        break;
                    }
                }
            } else if (assmeta.length > 0) {
                chosen = 0;
            }

            // Skipping if the desired assay isn't present for an experiment.
            if (chosen == null) {
                return;
            }

            let curass = assmeta[chosen];
            let mat_id = adb.packId(this.#unpacked.project, curass.resource.path, this.#unpacked.version);

            let promise = adb_utils.loadScranMatrix(baseUrl, mat_id, { getFun: this.constructor.#getFun, downloadFun: this.constructor.#downloadFun })
                .then(x => {
                    out_mat.add(modality_name, x.matrix);
                    out_rids[modality_name] = x.row_ids;
                    out_feats[modality_name] = bioc.SLICE(this.#rowdata[exp_name], x.row_ids);
                }
            );
            tasks.push(promise);
        };

        loader("RNA", this.#experimentRnaName, this.#countAssayRnaName);
        loader("ADT", this.#experimentAdtName, this.#countAssayAdtName);
        loader("CRISPR", this.#experimentCrisprName, this.#countAssayCrisprName);
        await Promise.all(tasks);

        // Assmebling the result.
        let output = { 
            cells: utils.cloneCached(this.#coldata, cache),
            matrix: out_mat,
            row_ids: out_rids,
            features: out_feats
        };

        // Setting the primary identifiers.
        let primaries = {
            RNA: this.#primaryRnaFeatureIdColumn,
            ADT: this.#primaryAdtFeatureIdColumn,
            CRISPR: this.#primaryCrisprFeatureIdColumn
        };
        for (const [k, v] of Object.entries(output.features)) {
            let id = primaries[k];
            if ((typeof id == "string" && v.hasColumn(id)) || (typeof id == "number" && id < v.numberOfColumns())) {
                v.$setRowNames(v.column(id));
            }
        }

        if (!cache) {
            this.clear();
        }
        return output;
    }

    /**
     * @return {object} Object describing this dataset, containing:
     *
     * - `files`: Array of objects representing the files used in this dataset.
     *   Each object corresponds to a single file and contains:
     *   - `type`: a string denoting the type.
     *   - `file`: a {@linkplain SimpleFile} object representing the file contents.
     * - `options`: An object containing additional options to saved.
     */
    serialize() {
        const enc = new TextEncoder;
        let buffer = enc.encode(this.#id);

        // Storing it as a string in the buffer.
        let output = {
            type: "id",
            file: new bakana.SimpleFile(buffer, { name: "id" })
        };

        return {
            files: [ output ],
            options: this.#dump_options()
        }
    }

    /**
     * @param {Array} files - Array of objects like that produced by {@linkcode CollaboratorDBDataset#serialize serialize}.
     * @param {object} options - Object containing additional options to be passed to the constructor.
     * @return {CollaboratorDBDataset} A new instance of this class.
     * @static
     */
    static unserialize(files, options) {
        let args = {};

        // This should contain 'id'.
        for (const x of files) {
            const dec = new TextDecoder;
            args[x.type] = dec.decode(x.file.buffer());
        }

        if (!("id" in args)) {
            throw new Error("expected a file of type 'id' when unserializing CollaboratorDB dataset"); 
        }
        return new CollaboratorDBDataset(args.id, options);
    }
}
