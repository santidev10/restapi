class UserFinalizeResponse:
    """
    Attempts to add token to response from request data
    """

    def finalize_response(self, request, response, *args, **kwargs):
        try:
            response.data.update({
                "token": request.auth.key,
                "device_id": request.auth.device_id,
            })
        except AttributeError:
            pass
        return super().finalize_response(request, response, *args, **kwargs)
