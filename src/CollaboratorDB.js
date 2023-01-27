import * as scran from "scran.js";
import * as bioc from "bioconductor";
import * as bakana from "bakana";
import * as adb from "artifactdb";
import * as utils from "./utils.js";
import * as adb_utils from "./ArtifactDB.js";

const baseUrl = "https://collaboratordb.aaron-lun.dev";

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
    #counts;

    #featureTypeRnaName;
    #featureTypeAdtName;
    #featureTypeCrisprName;

    #primaryRnaFeatureIdColumn;
    #primaryAdtFeatureIdColumn;
    #primaryCrisprFeatureIdColumn;

    #dump_options() {
        return {
            featureTypeRnaName: this.#featureTypeRnaName,
            featureTypeAdtName: this.#featureTypeAdtName,
            featureTypeCrisprName: this.#featureTypeCrisprName,
            primaryRnaFeatureIdColumn: this.#primaryRnaFeatureIdColumn,
            primaryAdtFeatureIdColumn: this.#primaryAdtFeatureIdColumn,
            primaryCrisprFeatureIdColumn: this.#primaryCrisprFeatureIdColumn
        };
    }

    /****************************************
     ****************************************/

    /**
     * @param {string} id - Identifier of a SummarizedExperiment in CollaboratorDB.
     * @param {object} [options={}] - Optional parameters.
     * @param {?string} [options.countAssayName=null] - See {@linkcode CollaboratorDBDataset#setCountAssayName setCountAssayName}.
     * @param {?string} [options.featureTypeRnaName=""] - See {@linkcode CollaboratorDBDataset#setFeatureTypeRnaName setFeatureTypeRnaName}.
     * @param {?string} [options.featureTypeAdtName="Antibody Capture"] - See {@linkcode CollaboratorDBDataset#setFeatureTypeAdtName setFeatureTypeAdtName}.
     * @param {?string} [options.featureTypeCrisprName="CRISPR Guide Capture"] - See {@linkcode CollaboratorDBDataset#setFeatureTypeCrisprName setFeatureTypeCrisprName}.
     * @param {string|number} [options.primaryRnaFeatureIdColumn=0] - See {@linkcode CollaboratorDBDataset#setPrimaryRnaFeatureIdColumn setPrimaryRnaFeatureIdColumn}.
     * @param {string|number} [options.primaryAdtFeatureIdColumn=0] - See {@linkcode CollaboratorDBDataset#setPrimaryAdtFeatureIdColumn setPrimaryAdtFeatureIdColumn}.
     * @param {string|number} [options.primaryCrisprFeatureIdColumn=0] - See {@linkcode CollaboratorDBDataset#setPrimaryCrisprFeatureIdColumn setPrimaryCrisprFeatureIdColumn}.
     */
    constructor(id, { 
        countAssayName = null, 
        featureTypeRnaName = null,
        featureTypeAdtName = "Antibody Capture", 
        featureTypeCrisprName = "CRISPR Guide Capture", 
        primaryRnaFeatureIdColumn = 0, 
        primaryAdtFeatureIdColumn = 0,
        primaryCrisprFeatureIdColumn = 0 
    } = {}) {

        this.#id = id;
        this.#unpacked = adb.unpackId(id);

        this.#countAssayName = countAssayName;

        this.#featureTypeRnaName = featureTypeRnaName;
        this.#featureTypeAdtName = featureTypeAdtName;
        this.#featureTypeCrisprName = featureTypeCrisprName;

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
     * This may be called at any time but only has an effect if `cache = true` in {@linkcode ExperimentHubDataset#load load} or {@linkcodeExperimentHubDataset#annotations annotations}. 
     */
    clear() {
        for (const v of Object.values(this.#counts)) {
            scran.free(v);
        }
        this.#counts = null;
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
     * @param {?string} name - Name of the assay containing the count matrix.
     * If `null`, the first encountered assay is used.
     */
    setCountAssayName(name) {
        this.#countAssayName = name;
        return;
    }

    /**
     * @param {?string} name - Name of the feature type for gene expression.
     * Alternatively `null`, to indicate that no RNA features are to be loaded.
     */
    setFeatureTypeRnaName(name) {
        this.#featureTypeRnaName = name;
        return;
    }

    /**
     * @param {string} name - Name of the feature type for ADTs.
     * Alternatively `null`, to indicate that no ADT features are to be loaded.
     */
    setFeatureTypeAdtName(name) {
        this.#featureTypeAdtName = name;
        return;
    }

    /**
     * @param {?string} name - Name of the feature type for CRISPR guides.
     * Alternatively `null`, to indicate that no guides are to be loaded.
     */
    setFeatureTypeCrisprName(name) {
        this.#featureTypeCrisprName = name;
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

        this.#metadata = await adb.getFileMetadata(baseUrl, this.#id);
        this.#alternatives = {};

        if ("single_cell_experiment" in meta) {
            let scmeta = meta.single_cell_experiment;
            if ("alternative_experiments" in scmeta) {
                let collected = [];
                let names = [];
                for (const x of scmeta.alternative_experiments) {
                    names.push(x.name);
                    let alt_id = adb.packId(unpacked.project, x.resource.id, unpacked.version);
                    collected.push(adb.getFileMetadata(baseUrl, alt_id));
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
        if (!("summarized_experiment" in meta)) {
            throw new Error("no 'summarized_experiment' property present in the metadata");
        }
        let smeta = meta.summarized_experiment;

        if ("column_data" in smeta) {
            let df_id = adb.packId(unpacked.project, smeta.column_data.resource.id, unpacked.version);
            this.#coldata = await adb_utils.loadDataFrame(baseUrl, df_id);
        } else {
            this.#coldata = new bioc.DataFrame({}, { numberOfRows: smeta.dimensions[1] });
        }

        return;
    }

    derive_default_name(meta) {
        let default_name = "";
        if ("single_cell_experiment" in meta) {
            let scmeta = meta.single_cell_experiment;
            if ("main_experiment_name" in scmeta) {
                default_name = scmeta.main_experiment_name;
            }
        }
        return default_name;
    }

    async #features() {
        if (this.#coldata !== null) {
            return;
        }

        await this.#core();
        let meta = this.#metadata;
        if (!("summarized_experiment" in meta)) {
            throw new Error("no 'summarized_experiment' property present in the metadata");
        }

        let output = {};
        let loader = async (se_meta, se_name) => {
            if ("row_data" in se_meta) {
                let df_id = adb.packId(unpacked.project, se_meta.row_data.resource.id, unpacked.version);
                output[se_name] = await adb_utils.loadDataFrame(baseUrl, df_id);
            } else {
                output[se_name] = new bioc.DataFrame({}, { numberOfRows: meta.summarized_experiment.dimensions[0] });
            }
        };

        await loader(meta.summarized_experiment, derive_default_name(meta));
        for (const [k, v] of Object.entries(this.#alternatives)) {
            await loader(v, k);
        }

        this.#rowdata = output;
        return;
    }

    async #counts() {
        if (this.#counts_handle !== null) {
            return;
        }

        let details = registry[this.#id];
        let counts_deets = await utils.downloadFun(baseUrl + "/" + details.counts);
        try {
            this.#counts_loaded = scran.readRds(counts_deets);
            this.#counts_handle = this.#counts_loaded.value();
        } catch(e) {
            scran.free(this.#counts_handle);
            scran.free(this.#counts_loaded);
            throw e;
        }
    }

    async #features() {
        if (this.#rowdata !== null) {
            return;
        }

        let details = registry[this.#id];

        if ("rowdata" in details) {
            let rowdata_deets = await utils.downloadFun(baseUrl + "/" + details.rowdata);

            let rowdata_load;
            let rowdata_handle;
            try {
                rowdata_load = scran.readRds(rowdata_deets);
                rowdata_handle = rowdata_load.value();
                let rowdata = load_data_frame(rowdata_handle);
                let names = rowdata.row_names;

                let output = {};
                if (names) {
                    output.id = names;
                }

                for (const [k, v] of Object.entries(rowdata.columns)) {
                    if (k.match(/^sym/)) {
                        output[k] = v;
                    }
                }

                if (Object.keys(output).length == 0) {
                    throw new Error("no acceptable feature identifiers found in the rowData DataFrame");
                }
                this.#rowdata = new bioc.DataFrame(output);
            } finally {
                scran.free(rowdata_handle);
                scran.free(rowdata_load);
            }
            return;
        }

        // Otherwise we pull the details from the counts.
        await this.#counts();
        let ids = extract_matrix_rownames(this.#counts_handle);
        this.#rowdata = new bioc.DataFrame({ id: ids });
    }

    /**
     * @param {object} [options={}] - Optional parameters.
     * @param {boolean} [options.cache=false] - Whether to cache the results for re-use in subsequent calls to this method or {@linkcode ExperimentHubDataset#load load}.
     * If `true`, users should consider calling {@linkcode ExperimentHubDataset#clear clear} to release the memory once this dataset instance is no longer needed.
     * 
     * @return {object} Object containing the per-feature and per-cell annotations.
     * This has the following properties:
     *
     * - `modality_features`: an object where each key is a modality name and each value is a {@linkplain external:DataFrame DataFrame} of per-feature annotations for that modality.
     *   Unlike {@linkcode ExperimentHubDataset#load load}, modality names are arbitrary.
     * - `modality_assay_names`: an object where each key is a modality name and each value is an Array of assay names for that modality.
     *   This should match the modality names in `modality_features`.
     * - `cells`: a {@linkplain external:DataFrame DataFrame} of per-cell annotations.
     */
    async summary({ cache = false } = {}) {
        await this.#features();
        await this.#cells();

        let output = { cells: utils.cloneCached(this.#coldata, cache) };
        let my_rd = utils.cloneCached(this.#rowdata, cache);
        output.modality_features = { "RNA": my_rd };

        if (!cache) {
            this.clear();
        }
        return output;
    }

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
     * either in the constructor or in setters like {@linkcode setFeatureTypeRnaName}.
     */
    async load({ cache = false } = {}) {
        await this.#features();
        await this.#cells();
        await this.#counts();

        let output = {
            cells: utils.cloneCached(this.#coldata, cache)
        };

        // Hard-coding the fact that we're dealing with RNA here, as all
        // registry entries are currently RNA-only anyway.
        let details = registry[this.#id];
        try {
            output.matrix = new scran.MultiMatrix;
            let counts = scran.initializeSparseMatrixFromRds(this.#counts_handle, { consume: !cache });

            output.matrix.add("RNA", counts.matrix);
            output.row_ids = { "RNA": counts.row_ids };

            let perm_features = bioc.SLICE(this.#rowdata, counts.row_ids);
            output.features = { "RNA": perm_features };
        } catch (e) {
            scran.free(output.matrix);
            throw e;
        }

        // Setting the primary identifiers.
        let curfeat = output.features["RNA"];
        let id = this.primaryRnaFeatureIdColumn;
        if ((typeof id == "string" && curfeat.hasColumn(id)) || (typeof id == "number" && id < curfeat.numberOfColumns())) {
            curfeat.$setRowNames(curfeat.column(id));
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
     * @param {Array} files - Array of objects like that produced by {@linkcode ExperimentHubDataset#serialize serialize}.
     * @param {object} options - Object containing additional options to be passed to the constructor.
     * @return {ExperimentHubDataset} A new instance of this class.
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
            throw new Error("expected a file of type 'id' when unserializing ExperimentHub dataset"); 
        }
        return new ExperimentHubDataset(args.id, options);
    }
}
