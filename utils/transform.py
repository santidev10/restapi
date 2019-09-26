def populate_video_custom_transcripts(video, transcript_texts, transcript_languages):
    if not transcript_texts or not transcript_languages:
        video.populate_custom_transcripts(transcripts_checked=True)
        return
    if len(transcript_texts) != len(transcript_languages):
        raise Exception("Len of transcript_texts and transcript_languages must be equal.")
    transcripts = [
        dict(
            text=transcript_texts[i],
            language_code=transcript_languages[i]
        )
        for i in range(len(transcript_texts))
    ]
    video.populate_custom_transcripts(transcripts_checked=True, transcripts=transcripts)
