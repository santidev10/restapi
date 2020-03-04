class SegmentFunctionalityMixin(object):
    def create_segment_relations(self, relation_model, segment, related_ids):
        for related_id in related_ids:
            relation_model.objects.create(
                segment=segment, related_id=related_id)