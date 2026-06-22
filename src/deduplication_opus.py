from nemo_curator.core.client import RayClient
from nemo_curator.stages.deduplication.fuzzy.workflow import FuzzyDeduplicationWorkflow
from nemo_curator.stages.text.deduplication.removal_workflow import TextDuplicatesRemovalWorkflow

def main():
    try:
        ray_client = RayClient()
        ray_client.start()

        # identify duplicate canidates first
        fuzzy_workflow = FuzzyDeduplicationWorkflow(
            input_path="/datasets/opus_es_cleaned",
            cache_path="./cache",
            output_path="/datasets/opus_es_deduplicated",
            text_field="content",
            perform_removal=False,
            input_filetype="parquet",
            input_blocksize="2GiB",
            seed=42,
            char_ngrams=24,
            num_bands=20,
            minhashes_per_band=13
        )

        fuzzy_workflow.run()

        # remove duplicate canidates
        removal_workflow = TextDuplicatesRemovalWorkflow(
            input_path="/datasets/opus_es_cleaned",
            ids_to_remove_path="/datasets/opus_es_deduplicated/FuzzyDuplicateIds",
            output_path="/opus_output/opus_es_final",
            input_filetype="parquet",
            input_blocksize="2GiB",
            input_id_field="_curator_dedup_id",
            ids_to_remove_duplicate_id_field="_curator_dedup_id",
            id_generator_path="/datasets/opus_es_deduplicated/fuzzy_id_generator.json"
        )

        removal_workflow.run()
    except Exception as e:
        print(f"Error in deduplicaton pipeline: {e}")
        return
    finally:
        ray_client.stop()

if __name__ == "__main__":
    main()