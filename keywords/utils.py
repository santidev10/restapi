class KeywordViralParamAdapter:
    parameter_full_name = "stats.is_viral"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_full_name)
        if parameter:
            if parameter == "Viral":
                query_params[self.parameter_full_name] = "true"
            elif parameter == "All":
                query_params[self.parameter_full_name] = ""
        return query_params
