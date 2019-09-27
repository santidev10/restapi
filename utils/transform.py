def populate_video_custom_transcripts(video, transcript_texts=[], transcript_languages=[]):
    if len(transcript_texts) != len(transcript_languages):
        raise Exception("Len of transcript_texts and transcript_languages must be equal.")
    transcripts = [
        dict(
            text=text,
            language_code=language_code
        )
        for text, language_code in zip(transcript_texts, transcript_languages)
    ]
    video.populate_custom_transcripts(transcripts_checked=True, transcripts=transcripts)
