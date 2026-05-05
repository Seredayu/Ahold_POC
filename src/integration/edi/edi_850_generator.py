import os
from dataclasses import dataclass
from datetime import datetime

from azure.storage.blob import BlobServiceClient


@dataclass
class EDI850Line:
    po_number: str
    line_number: int
    ean_barcode: str
    quantity: int
    unit: str
    unit_price: float
    requested_ship_date: str  # YYYYMMDD


class EDI850Generator:
    """Generates EDI 850 Purchase Order files and lands them in gold.edi.outbound/.

    Azure Logic Apps picks up files from Blob Storage — EDI is never routed
    directly from Databricks to external SFTP (security boundary).
    """

    BLOB_CONTAINER = "gold-edi-outbound"

    def __init__(self, connection_string: str | None = None):
        conn_str = connection_string or os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        self._blob_client = BlobServiceClient.from_connection_string(conn_str)

    def generate(self, lines: list[EDI850Line], sender_id: str, receiver_id: str) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        control_number = timestamp[-9:]
        content = self._build_x12(lines, sender_id, receiver_id, control_number, timestamp)
        filename = f"EDI850_{lines[0].po_number}_{timestamp}.edi"
        self._upload(filename, content)
        return filename

    def _upload(self, filename: str, content: str) -> None:
        container = self._blob_client.get_container_client(self.BLOB_CONTAINER)
        container.upload_blob(name=filename, data=content.encode(), overwrite=True)

    def _build_x12(
        self,
        lines: list[EDI850Line],
        sender_id: str,
        receiver_id: str,
        control_number: str,
        timestamp: str,
    ) -> str:
        date = timestamp[:8]
        time_ = timestamp[8:12]
        segments = [
            f"ISA*00*          *00*          *ZZ*{sender_id:<15}*ZZ*{receiver_id:<15}*{date}*{time_}*^*00501*{control_number}*0*P*:",
            f"GS*PO*{sender_id}*{receiver_id}*{date}*{time_}*1*X*005010",
            f"ST*850*0001",
            f"BEG*00*NE*{lines[0].po_number}**{date}",
        ]
        for line in lines:
            segments += [
                f"PO1*{line.line_number}*{line.quantity}*{line.unit}*{line.unit_price}**UP*{line.ean_barcode}",
                f"DTM*010*{line.requested_ship_date}",
            ]
        segments += [
            f"CTT*{len(lines)}",
            f"SE*{len(segments) + 2}*0001",
            f"GE*1*1",
            f"IEA*1*{control_number}",
        ]
        return "\n".join(segments)
