def get_current_release():
    import subprocess
    import re
    try:
        branches = subprocess.check_output(["git", "branch", "--merged"]) \
            .decode("utf-8").split("\n")

        regexp = re.compile(r"release\/([\d.]+)")

        release_branches = filter(lambda b: regexp.search(b), branches)
        releases = [regexp.search(b).group(1) for b in release_branches]
        return sorted(releases, reverse=True)[0]
    except:
        return "0.0"
