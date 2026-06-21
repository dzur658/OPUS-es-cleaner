import re
from nemo_curator.stages.text.modifiers import DocumentModifier
from nemo_curator.core.client import RayClient
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import ParquetReader
from nemo_curator.stages.text.io.writer import ParquetWriter
from nemo_curator.stages.text.modules import Modify, ScoreFilter
from nemo_curator.stages.text.filters import WordCountFilter, FastTextLangId

# path to fasttext lang id model
FASTTEXT_MODEL_PATH = "/models/lid.176.bin"

# Regex patterns to remove

# risk keeping dash speaker delimeters, demonstrates turn based structure

# keep stage directions but drop brackets
stage_brackets_pattern = re.compile(r"^\[(.*)\]$")

# look behind to catch orphaned square brackets
orphaned_brackets_pattern = re.compile(r"(?<!\\)[\[\]]")

# strip timestamps
timestamp_pattern = re.compile(r"^\s*\d{1,2}:\d{2}(:\d{2})?([.,]\d+)?\s*$")

# strip leading and trailing timestamps
leading_timestamps_pattern = re.compile(r"^\s*\d{2},\s*\d{3}\s+")
trailing_timestamps_pattern = re.compile(r"\s*\d+\s*\d{2}:\s*$")

# catch any html or raw code that may be present in the dataset
html_tags_pattern = re.compile(r"<[^>]+>")
html_entities_pattern = re.compile(r"&\w+;|&#\d+;")

# catch all whitespace (we don't need newlines anymore since data is in seperated rows)
whitespace_pattern = re.compile(r"\s+")

class OPUSRegexCleaner(DocumentModifier):
    """
    Removes all undesirable CPT patterns in OPUS
    """
    def __init__(self):
        super().__init__()
    def modify_document(self, text: str) -> str:
        # remove stage brackets but keep content
        text = stage_brackets_pattern.sub(r"\1", text)

        # remove orphaned square brackets
        text = orphaned_brackets_pattern.sub("", text)

        # remove timestamps
        text = timestamp_pattern.sub("", text)

        # remove leading and trailing timestamps
        text = leading_timestamps_pattern.sub("", text)
        text = trailing_timestamps_pattern.sub("", text)

        # remove html tags and entities
        text = html_tags_pattern.sub("", text)
        text = html_entities_pattern.sub("", text)

        # convert all whitespaces to single spaces
        text = whitespace_pattern.sub(" ", text)

        # returned stripped text (removes leading and trailing whitespace)
        return text.strip()

def main():
    try:
        ray_client = RayClient()
        ray_client.start()

        # Create the pipelne
        pipeline = Pipeline(
            name="OPUS ES Cleaning Pipeline",
            description="Pipeline to clean the OPUS ES dataset using regex patterns and perform lang id filtering",
        )

        # load data with parquet reader
        pipeline.add_stage(ParquetReader(file_paths="/datasets/opus_es_downloaded"))

        # add regex cleaning to the pipeline
        pipeline.add_stage(Modify(
            OPUSRegexCleaner(),
            input_fields="content",
            output_fields="content"
            ))

        # perform length filtering prior to language identification
        pipeline.add_stage(ScoreFilter(
            filter_obj=WordCountFilter(min_words=3),
            text_field="content",
            score_field="word_count"
        ))

        # perform language identification filtering using fasttext
        pipeline.add_stage(ScoreFilter(
            FastTextLangId(model_path=FASTTEXT_MODEL_PATH, min_langid_score=0.7),
            text_field="content",
            score_field="language"
        ))

        # write the results to a clean parquet file
        pipeline.add_stage(ParquetWriter(path="/datasets/opus_es_cleaned"))

        # run the pipeline
        results = pipeline.run()

    except Exception as e:
        print(f"An error occured while running the cleaning pipeline: {e}")
        return
    finally:
        ray_client.stop()

if __name__ == "__main__":
    main()