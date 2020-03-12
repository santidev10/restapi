from rest_framework.exceptions import ValidationError
from rest_framework.serializers import CharField
from rest_framework.serializers import JSONField
from rest_framework.serializers import ModelSerializer

from userprofile.models import WhiteLabel


class WhiteLabelSerializer(ModelSerializer):
    domain = CharField(max_length=255, required=False)
    config = JSONField(required=False)

    class Meta:
        model = WhiteLabel
        fields = ("id", "domain", "config")

    def validate_config(self, config):
        """ Update config """
        updated = {
            **getattr(self.instance, "config", {})
        }
        updated.update(config)
        return updated

    def validate_domain(self, domain):
        errs = []
        if WhiteLabel.objects.filter(domain=domain).exists():
            raise ValidationError("Domain already exists.")
        try:
            if " " in domain:
                errs.append("Domain may not contain spaces.")
            if len(domain) > 63:
                errs.append("Domain may only be 63 characters or less.")
            if domain[0] == "-" or domain[-1] == "-":
                errs.append("Domain may not start or end with hyphens.")
            if not domain[0].isalpha:
                errs.append("Domain must start with a letter.")
        except (IndexError, TypeError):
            errs.append("You must provide a domain value.")
        if errs:
            raise ValidationError(errs)
        return domain
