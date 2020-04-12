class BaseApi:
    user_agent = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "X-NewRelic-ID": "VQECWF5UChAHUlNTBwgBVw==",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Connection": "keep-alive",
        "Referer": "https://stats.nba.com/",
    }
    base_url = "https://stats.nba.com/"

    def build_parameters_dict(self):
        """
        function to build the dictionary of parameters to pass to the url
        from the instance attributes
        """
        params_dict = {}
        for key, value in vars(self).items():
            if key in ["user_agent", "url"]:
                continue
            params_dict[key] = value

        return params_dict
