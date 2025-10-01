from .downloads import DDSDownload
from .items import DDSGetItems


class DDSClient(DDSGetItems, DDSDownload):
    """
    Full client combining get-items and download functionality.
    Multiple inheritance works since both share BaseDDSClient.
    """

    pass