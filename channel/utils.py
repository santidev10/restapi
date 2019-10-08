class ChannelGroupParamAdapter:
    parameter_full_name = "stats.channel_group"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_full_name)
        if parameter:
            parameter = parameter.lower().split(" ")[0]
            query_params[self.parameter_full_name] = parameter

        return query_params