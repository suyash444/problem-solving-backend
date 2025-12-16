"""
SQLAlchemy ORM Models for Problem Solving Tracker
Maps to database tables in ProblemSolvingTrackerDB
"""
from sqlalchemy import (
    Column, BigInteger, String, DateTime, Numeric, Integer, 
    Boolean, ForeignKey, Text, Date, func
)
from sqlalchemy.orm import relationship
from datetime import datetime

from .connection import Base


# ============================================================================
# IMPORT TABLES
# ============================================================================

class ImportDumptrack(Base):
    __tablename__ = 'import_dumptrack'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    Batch = Column(BigInteger)
    OrdinePrivalia = Column(String(50))
    DataRegistrazione = Column(DateTime)
    nLista = Column(BigInteger)
    CodiceArticolo = Column(String(80))
    QtaRichiestaTotale = Column(Numeric(18, 3))
    QtaPrelevata = Column(Numeric(18, 3))
    nListaComposta = Column(BigInteger)
    Commessa = Column(String(50))
    Utente = Column(String(50))
    DataPrelievo = Column(DateTime)
    UDC = Column(String(50))
    NCollo = Column(Integer)
    CodiceImballo = Column(String(50))
    DataOraArrivoPrivalia = Column(DateTime)
    LetteraVettura = Column(String(100))
    Vettore = Column(String(80))
    DataStampa = Column(DateTime)
    CodiceProprieta = Column(String(20))
    StatoArticolo = Column(String(50))
    Uds = Column(String(50))
    source_file = Column(String(500))
    imported_at = Column(DateTime, default=datetime.utcnow)


class ImportMonitor(Base):
    __tablename__ = 'import_monitor'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    DataOra = Column(DateTime)
    Movimento = Column(String(80))
    Pallet = Column(String(50))
    Articolo = Column(String(80))
    Descrizione = Column(String(255))
    Quantita = Column(Numeric(18, 4))
    LottoEntrata = Column(String(50))
    LottoConfezionamento = Column(String(50))
    Matricola = Column(String(50))
    LottoFornitore = Column(String(50))
    Made = Column(String(50))
    Mag = Column(String(20))
    Scaf = Column(String(20))
    Col = Column(String(20))
    Pia = Column(String(20))
    Sc = Column(String(20))
    Comp = Column(String(20))
    ListaRif = Column(String(50))
    DescrizioneBrand = Column(String(100))
    PackingList = Column(String(50))
    DataBolla = Column(DateTime)
    Tag = Column(String(50))
    CodiceProprieta = Column(String(20))
    Causaleprelievo = Column(String(100))
    CodicePallet = Column(String(50))
    Categoria = Column(String(50))
    CodiceCategoria = Column(String(20))
    EuroUDC = Column(Integer)
    Riga = Column(Integer)
    QtaCorrente = Column(Numeric(18, 4))
    DeltaQTA = Column(Numeric(18, 4))
    source_file = Column(String(500))
    imported_at = Column(DateTime, default=datetime.utcnow)


class ImportPrelievo(Base):
    __tablename__ = 'import_prelievo_powersort'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    Listone = Column(BigInteger)
    Carrello = Column(String(50))
    UDC = Column(String(50))
    CodiceArticolo = Column(String(80))
    Descrizione = Column(String(255))
    Quantita = Column(Numeric(18, 3))
    Utente = Column(String(50))
    DataPrelievo = Column(DateTime)
    CodiceProprieta = Column(String(20))
    Azienda = Column(String(100))
    imported_at = Column(DateTime, default=datetime.utcnow)


class ImportSpedito(Base):
    __tablename__ = 'import_spedito'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    CodiceProprieta = Column(String(20))
    Azienda = Column(String(100))
    Vettore = Column(String(80))
    Sovracollo = Column(String(50))
    nOrdine = Column(String(50))
    nLista = Column(BigInteger)
    CodiceArticolo = Column(String(80))
    Descrizione = Column(String(255))
    Quantita = Column(Numeric(18, 3))
    Cesta = Column(String(50))
    CodiceLetto = Column(String(80))
    DataOra = Column(DateTime)
    imported_at = Column(DateTime, default=datetime.utcnow)


# ============================================================================
# BUSINESS TABLES
# ============================================================================

class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_number = Column(String(50), unique=True, nullable=False)
    data_registrazione = Column(DateTime)
    commessa = Column(String(50))
    codice_proprieta = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    items = relationship("OrderItem", back_populates="order")


class OrderItem(Base):
    __tablename__ = 'order_items'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(BigInteger, ForeignKey('orders.id'), nullable=False)
    n_lista = Column(BigInteger, nullable=False)
    listone = Column(BigInteger)
    sku = Column(String(80), nullable=False)
    qty_ordered = Column(Numeric(18, 3), nullable=False)
    descrizione = Column(String(255))
    cesta = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    order = relationship("Order", back_populates="items")
    picking_events = relationship("PickingEvent", back_populates="order_item")


class PickingEvent(Base):
    __tablename__ = 'picking_events'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_item_id = Column(BigInteger, ForeignKey('order_items.id'), nullable=False)
    udc = Column(String(50), nullable=False)
    carrello = Column(String(50))
    qty_picked = Column(Numeric(18, 3), nullable=False)
    operator = Column(String(50))
    picked_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    order_item = relationship("OrderItem", back_populates="picking_events")


class UDCInventory(Base):
    __tablename__ = 'udc_inventory'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    udc = Column(String(50), nullable=False)
    sku = Column(String(80), nullable=False)
    listone = Column(BigInteger, nullable=False)
    qty = Column(Numeric(18, 3), nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow)


class UDCLocation(Base):
    __tablename__ = 'udc_locations'
    
    udc = Column(String(50), primary_key=True)
    mag = Column(String(20))
    scaf = Column(String(20))
    col = Column(String(20))
    pia = Column(String(20))
    sc = Column(String(20))
    comp = Column(String(20))
    position_code = Column(String(120), nullable=False)
    last_movement = Column(DateTime)
    last_updated = Column(DateTime, default=datetime.utcnow)


class ShippedItem(Base):
    __tablename__ = 'shipped_items'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    cesta = Column(String(500), nullable=False)
    n_ordine = Column(String(50), nullable=False)
    n_lista = Column(BigInteger, nullable=False)
    sku = Column(String(80), nullable=False)
    qty_shipped = Column(Numeric(18, 3), nullable=False)
    descrizione = Column(String(255))
    sovracollo = Column(String(50))
    vettore = Column(String(80))
    shipped_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


# ============================================================================
# MISSION TABLES
# ============================================================================

class Mission(Base):
    __tablename__ = 'missions'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    mission_code = Column(String(50), unique=True, nullable=False)
    cesta = Column(String(50), nullable=False)
    reference_n_lista = Column(BigInteger, nullable=True)
    status = Column(String(20), default='OPEN', nullable=False)
    created_by = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # Relationships
    items = relationship("MissionItem", back_populates="mission")
    checks = relationship("PositionCheck", back_populates="mission")


class MissionItem(Base):
    __tablename__ = 'mission_items'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    mission_id = Column(BigInteger, ForeignKey('missions.id'), nullable=False)
    cesta = Column(String(50), nullable=True)
    n_ordine = Column(String(50), nullable=False)
    n_lista = Column(BigInteger, nullable=False)
    sku = Column(String(80), nullable=False)
    listone = Column(Integer)
    qty_ordered = Column(Numeric(18, 3), nullable=False)
    qty_shipped = Column(Numeric(18, 3), nullable=False)
    qty_missing = Column(Numeric(18, 3), nullable=False)
    qty_found = Column(Numeric(18, 3), default=0, nullable=False)
    is_resolved = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime)
    
    # Relationships
    mission = relationship("Mission", back_populates="items")
    checks = relationship("PositionCheck", back_populates="mission_item")

class PositionCheck(Base):
    __tablename__ = 'position_checks'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    mission_id = Column(BigInteger, ForeignKey('missions.id'), nullable=False)
    mission_item_id = Column(BigInteger, ForeignKey('mission_items.id'), nullable=False)
    position_code = Column(String(120), nullable=False)
    udc = Column(String(50))
    listone = Column(BigInteger)
    status = Column(String(30), default='TO_CHECK', nullable=False)
    found_in_position = Column(Boolean)
    qty_found = Column(Numeric(18, 3))
    checked_at = Column(DateTime)
    checked_by = Column(String(50))
    notes = Column(String(500))
    
    # Relationships
    mission = relationship("Mission", back_populates="checks")
    mission_item = relationship("MissionItem", back_populates="checks")


# ============================================================================
# IMPORT LOG
# ============================================================================

class ImportLog(Base):
    __tablename__ = 'import_log'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_type = Column(String(50), nullable=False)
    file_path = Column(String(500), nullable=True, default='')
    file_hash = Column(String(64), nullable=False)
    file_date = Column(Date, nullable=True)
    records_imported = Column(Integer, nullable=True, default=0)
    import_started_at = Column(DateTime, default=datetime.utcnow)
    import_completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), default='SUCCESS', nullable=False)
    error_message = Column(String(500), nullable=True, default='')  # Changed from Text