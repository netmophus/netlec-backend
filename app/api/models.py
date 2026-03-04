from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


Role = Literal["admin", "supervisor", "agent", "customer"]


class ZoneRef(BaseModel):
    center: str
    zone: str
    sector: str


class UserPublic(BaseModel):
    id: str = Field(alias="_id")
    phone: str
    name: str | None = None
    role: Role
    isActive: bool
    meterNumber: str | None = None
    subscriberNumber: str | None = None
    police: str | None = None
    oldIndex: int | None = None
    tariffCode: str | None = None
    category: str | None = None
    grouping: str | None = None
    address: str | None = None
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    assignedZones: list[ZoneRef] | None = None


class RegisterRequest(BaseModel):
    phone: str
    name: str | None = None
    password: str


class RegisterLookupRequest(BaseModel):
    phone: str


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    mustChangePassword: bool = False


class PortalAnnouncementPublic(BaseModel):
    id: str
    title: str
    message: str
    date: str


class PortalSettingsPublic(BaseModel):
    logoUrl: str | None = None
    facebookUrl: str | None = None
    linkedinUrl: str | None = None
    xUrl: str | None = None
    youtubeUrl: str | None = None
    supportPhone: str | None = None
    supportWhatsapp: str | None = None
    latestAnnouncements: list[PortalAnnouncementPublic] = []


class ChangePasswordRequest(BaseModel):
    currentPassword: str
    newPassword: str


class CreateStaffUserRequest(BaseModel):
    phone: str
    name: str
    password: str
    role: Literal["supervisor", "agent"]
    isActive: bool = True


class CreateAgentBySupervisorRequest(BaseModel):
    phone: str
    name: str
    password: str
    center: str
    zone: str
    sector: str
    isActive: bool = True


class UpdateUserRequest(BaseModel):
    name: str | None = None
    role: Role | None = None
    isActive: bool | None = None
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    assignedZones: list[ZoneRef] | None = None


class PreRegisterCustomerRequest(BaseModel):
    phone: str
    meterNumber: str
    subscriberNumber: str
    police: str

    name: str | None = None
    address: str | None = None
    tariffCode: str | None = None
    category: str | None = None
    grouping: str | None = None
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    source: Literal["SI_IMPORT", "BACKOFFICE"] = "BACKOFFICE"


class ImportCustomersResponse(BaseModel):
    inserted: int
    updated: int
    skipped: int
    errors: int
    errorLines: list[int] = []


class ImportMetersResponse(BaseModel):
    inserted: int
    updated: int
    skipped: int
    errors: int
    errorLines: list[int] = []


class AdminStatsResponse(BaseModel):
    internalUsers: int
    preRegisteredCustomers: int


class UserInDB(BaseModel):
    phone: str
    name: str | None = None
    role: Role
    passwordHash: str | None = None
    isActive: bool
    mustChangePassword: bool = False
    meterNumber: str | None = None
    subscriberNumber: str | None = None
    police: str | None = None
    oldIndex: int | None = None
    tariffCode: str | None = None
    category: str | None = None
    grouping: str | None = None
    address: str | None = None
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    assignedZones: list[ZoneRef] | None = None
    source: str | None = None
    preRegisteredBy: str | None = None
    createdAt: datetime
    updatedAt: datetime


class MeterPublic(BaseModel):
    id: str = Field(alias="_id")
    meterNumber: str
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    routeOrder: int | None = None
    subscriberNumber: str | None = None
    police: str | None = None
    address: str | None = None


class MeterInDB(BaseModel):
    meterNumber: str
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    routeOrder: int | None = None
    subscriberNumber: str | None = None
    police: str | None = None
    address: str | None = None
    createdAt: datetime
    updatedAt: datetime


class TourItem(BaseModel):
    meterId: str | None = None
    meterNumber: str
    routeOrder: int | None = None
    oldIndex: int | None = None


class CreateReadingRequest(BaseModel):
    tourId: str
    date: str
    meterNumber: str
    newIndex: int = Field(ge=0, le=999999999)
    gps: dict | None = None
    gpsMissing: bool = False
    gpsMissingReason: str | None = None


class ReadingOcrRequest(BaseModel):
    imageUrl: str
    oldIndex: int | None = Field(default=None, ge=0, le=999999999)


class ReadingOcrResponse(BaseModel):
    provider: str
    rawText: str | None = None
    proposedIndex: str | None = None
    confidence: float | None = None


class UpdateReadingRequest(BaseModel):
    newIndex: int = Field(ge=0, le=999999999)
    gps: dict | None = None
    gpsMissing: bool | None = None
    gpsMissingReason: str | None = None


class ReadingPublic(BaseModel):
    id: str = Field(alias="_id")
    date: str
    tourId: str
    agentId: str
    meterNumber: str
    oldIndex: int | None = None
    newIndex: int
    consumption: int | None = None
    tariffCode: str | None = None
    amount: int | None = None
    createdAt: datetime
    updatedAt: datetime


class ReadingWithLocationPublic(ReadingPublic):
    center: str | None = None
    zone: str | None = None
    sector: str | None = None


class AgentReadingSummaryItem(BaseModel):
    tourId: str
    meterNumber: str
    date: str


class BillingLineItem(BaseModel):
    date: str
    meterNumber: str
    oldIndex: int | None = None
    newIndex: int
    consumption: int | None = None
    tariffCode: str | None = None
    amount: int | None = None
    createdAt: datetime


class CustomerBillingResponse(BaseModel):
    meterNumber: str | None = None
    tariffCode: str | None = None
    totalConsumption: int | None = None
    totalAmount: int | None = None
    items: list[BillingLineItem] = []


class InvoicePublic(BaseModel):
    id: str
    period: str
    date: str
    dueDate: str | None = None
    meterNumber: str
    tariffCode: str | None = None
    consumption: int | None = None
    amount: int | None = None
    status: str
    readingId: str


class InitiatePaymentRequest(BaseModel):
    invoiceId: str
    provider: str = Field(pattern="^(NITA|BANK_TRANSFER|PISPI)$")


class PaymentPublic(BaseModel):
    id: str = Field(alias="_id")
    invoiceId: str
    customerId: str
    provider: str
    amount: int | None = None
    status: str
    createdAt: datetime
    updatedAt: datetime


class TourPublic(BaseModel):
    id: str = Field(alias="_id")
    date: str
    center: str
    zone: str
    sector: str
    agentId: str
    items: list[TourItem]
    createdAt: datetime
    updatedAt: datetime


class TourInDB(BaseModel):
    date: str
    center: str
    zone: str
    sector: str
    agentId: str
    items: list[TourItem]
    createdAt: datetime
    updatedAt: datetime


class GenerateToursAssignment(BaseModel):
    agentId: str
    count: int = Field(ge=0, le=500)


class GenerateToursRequest(BaseModel):
    date: str
    mode: str = Field(pattern="^(A|B2|MANUAL)$")
    center: str | None = None
    zone: str | None = None
    sector: str | None = None
    agentId: str | None = None
    agentIds: list[str] | None = None
    maxMetersPerTour: int | None = Field(default=None, ge=1, le=500)
    assignments: list[GenerateToursAssignment] | None = None


class GenerateToursResponse(BaseModel):
    created: int
    skipped: int
    errors: int
    errorLines: list[str] = []
    tours: list[TourPublic] = []


class CreateZoneRequest(BaseModel):
    center: str
    zone: str
    sector: str


class ZonePublic(BaseModel):
    id: str = Field(alias="_id")
    center: str
    zone: str
    sector: str
    createdAt: datetime
    updatedAt: datetime


class ZoneInDB(BaseModel):
    center: str
    zone: str
    sector: str
    createdAt: datetime
    updatedAt: datetime


class TariffPublic(BaseModel):
    id: str = Field(alias="_id")
    code: str
    fromKwh: int = Field(ge=1, le=1000000000)
    toKwh: int | None = Field(default=None, ge=1, le=1000000000)
    ratePerKwh: int = Field(ge=0, le=1000000)
    createdAt: datetime
    updatedAt: datetime


class TariffUpsertItem(BaseModel):
    code: str
    fromKwh: int = Field(ge=1, le=1000000000)
    toKwh: int | None = Field(default=None, ge=1, le=1000000000)
    ratePerKwh: int = Field(ge=0, le=1000000)


class UpsertTariffsRequest(BaseModel):
    items: list[TariffUpsertItem] | None = None
