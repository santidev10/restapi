def populate_video_custom_captions(video, transcript_texts=[], transcript_languages=[], source=None):
    if len(transcript_texts) != len(transcript_languages):
        raise Exception("Len of transcript_texts and transcript_languages must be equal.")
    transcripts = [
        dict(
            text=text,
            language_code=language_code,
            source=source
        )
        for text, language_code in zip(transcript_texts, transcript_languages) if text != ""
    ]
    video.populate_custom_captions(transcripts_checked=True, items=transcripts)
