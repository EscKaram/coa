-- Flight Service: справочники авиакомпаний и аэропортов, рейсы, резервации мест.

CREATE TYPE flight_status AS ENUM ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED');
CREATE TYPE reservation_status AS ENUM ('ACTIVE', 'RELEASED', 'EXPIRED');

-- updated_at выставляется триггером при каждом UPDATE строки.
CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE airlines (
    iata_code CHAR(2)      PRIMARY KEY CHECK (iata_code ~ '^[A-Z0-9]{2}$'),
    name      VARCHAR(100) NOT NULL CHECK (btrim(name) <> '')
);

CREATE TABLE airports (
    iata_code CHAR(3)      PRIMARY KEY CHECK (iata_code ~ '^[A-Z]{3}$'),
    name      VARCHAR(100) NOT NULL CHECK (btrim(name) <> ''),
    city      VARCHAR(100) NOT NULL CHECK (btrim(city) <> '')
);

-- Номер рейса хранится как код авиакомпании + число (SU + 1234), а не строкой "SU1234":
-- иначе авиакомпания зависела бы от части ключа (номер рейса, дата), что нарушает 2NF/3NF.
-- available_seats — счётчик, который требует задание. Он меняется только вместе с
-- seat_reservations в одной транзакции под SELECT ... FOR UPDATE.
CREATE TABLE flights (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_code    CHAR(2)        NOT NULL REFERENCES airlines (iata_code),
    flight_no       SMALLINT       NOT NULL CHECK (flight_no BETWEEN 1 AND 9999),
    origin          CHAR(3)        NOT NULL REFERENCES airports (iata_code),
    destination     CHAR(3)        NOT NULL REFERENCES airports (iata_code),
    departure_time  TIMESTAMPTZ    NOT NULL,
    arrival_time    TIMESTAMPTZ    NOT NULL,
    total_seats     INTEGER        NOT NULL CHECK (total_seats > 0),
    available_seats INTEGER        NOT NULL CHECK (available_seats >= 0),
    price           NUMERIC(12, 2) NOT NULL CHECK (price > 0),
    currency        CHAR(3)        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status          flight_status  NOT NULL DEFAULT 'SCHEDULED',
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT flights_available_le_total CHECK (available_seats <= total_seats),
    CONSTRAINT flights_route_distinct CHECK (origin <> destination),
    CONSTRAINT flights_arrival_after_departure CHECK (arrival_time > departure_time)
);

-- Один рейс (номер) в одну дату вылета. Дата берётся по UTC.
CREATE UNIQUE INDEX uq_flights_number_departure_date
    ON flights (airline_code, flight_no, ((departure_time AT TIME ZONE 'UTC')::date));

-- SearchFlights: маршрут + диапазон времени вылета, только SCHEDULED.
CREATE INDEX idx_flights_route_departure
    ON flights (origin, destination, departure_time)
    WHERE status = 'SCHEDULED';

CREATE TRIGGER flights_set_updated_at
    BEFORE UPDATE ON flights
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Одному бронированию соответствует ровно одна резервация: UNIQUE (booking_id).
-- booking_id ссылается на bookings.id в БД Booking Service, поэтому внешнего ключа нет.
CREATE TABLE seat_reservations (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_id  UUID               NOT NULL REFERENCES flights (id),
    booking_id UUID               NOT NULL UNIQUE,
    seat_count INTEGER            NOT NULL CHECK (seat_count > 0),
    status     reservation_status NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ        NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ        NOT NULL DEFAULT now()
);

CREATE INDEX idx_seat_reservations_flight_id ON seat_reservations (flight_id);

CREATE TRIGGER seat_reservations_set_updated_at
    BEFORE UPDATE ON seat_reservations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
