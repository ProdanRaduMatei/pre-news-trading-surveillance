from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TickerReference:
    ticker: str
    cik: str
    company_name: str
    source_url: str
    retrieved_at: str

    def as_db_row(self) -> tuple[str, str, str, str, str]:
        return (
            self.ticker,
            self.cik,
            self.company_name,
            self.source_url,
            self.retrieved_at,
        )

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class RawFilingRecord:
    filing_id: str
    ticker: str
    cik: str
    company_name: str
    accession_no: str
    form_type: str
    filing_date: str | None
    accepted_at: str | None
    items_json: str | None
    primary_document: str | None
    primary_doc_description: str | None
    source_url: str
    raw_path: str
    ingested_at: str

    def as_db_row(
        self,
    ) -> tuple[
        str,
        str,
        str,
        str,
        str,
        str,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
        str,
        str,
        str,
    ]:
        return (
            self.filing_id,
            self.ticker,
            self.cik,
            self.company_name,
            self.accession_no,
            self.form_type,
            self.filing_date,
            self.accepted_at,
            self.items_json,
            self.primary_document,
            self.primary_doc_description,
            self.source_url,
            self.raw_path,
            self.ingested_at,
        )

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)
