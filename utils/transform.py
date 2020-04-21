def populate_video_custom_captions(video, transcript_texts=[], transcript_languages=[], source=None, asr_lang=None):
    if len(transcript_texts) != len(transcript_languages):
        raise Exception("Len of transcript_texts and transcript_languages must be equal.")
    transcripts = [
        dict(
            text=text,
            language_code=language_code,
            source=source,
            is_asr=True if language_code == asr_lang else False
        )
        for text, language_code in zip(transcript_texts, transcript_languages) if text != ""
    ]
    if source == "tts_url":
        video.populate_custom_captions(transcripts_checked_tts_url=True, items=transcripts)
    else:
        video.populate_custom_captions(transcripts_checked_v2=True, items=transcripts)
