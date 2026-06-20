"""External data catalog integration (PRD Phase 8).

r2g connects to *external* enterprise data catalogs (OpenMetadata, and later
AWS Glue / Atlan / DataHub) and uses them as an upstream **discovery** layer:
browse the catalog, select a database/schema (or Kafka topic), and register it
as an r2g migration source. r2g still connects to the underlying data store
itself to perform the migration ("discover-then-connect").

This is distinct from r2g's *internal* catalog (`r2g.catalog`), which stores
r2g's own sources / projects / mappings.
"""
