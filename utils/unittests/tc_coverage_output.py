from lxml import etree
from teamcity.messages import TeamcityServiceMessages


class MessageService(TeamcityServiceMessages):
    def buildStatisticLinesRate(self, linesRate):
        self.message("buildStatisticValue", key="CodeCoverageL", value=str(linesRate))

    def buildStatisticBranchesCovered(self, branchesCovered):
        self.message("buildStatisticValue", key="CodeCoverageAbsBCovered", value=str(branchesCovered))

    def buildStatisticTotalBranches(self, totalBranches):
        self.message("buildStatisticValue", key="CodeCoverageAbsBTotal", value=str(totalBranches))

    def buildStatisticBranchesRate(self, branchesRate):
        self.message("buildStatisticValue", key="CodeCoverageB", value=str(branchesRate))


def parse():
    msg_service = MessageService()
    tree = etree.parse("coverage.xml")
    node = tree.xpath("//coverage")[0]
    msg_service.buildStatisticLinesCovered(node.get("lines-covered"))
    msg_service.buildStatisticTotalLines(node.get("lines-valid"))
    msg_service.buildStatisticLinesRate(float(node.get("line-rate")) * 100)

    msg_service.buildStatisticBranchesCovered(node.get("branches-covered"))
    msg_service.buildStatisticTotalBranches(node.get("branches-valid"))
    msg_service.buildStatisticBranchesRate(float(node.get("branch-rate")) * 100)


if __name__ == "__main__":
    parse()
