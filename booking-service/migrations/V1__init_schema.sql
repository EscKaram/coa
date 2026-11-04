-- Booking Service: бронирования.

CREATE TYPE booking_status AS ENUM ('CONFIRMED', 'CANCELLED');

CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- id генерирует Booking Service до вызова ReserveSeats: он же ключ идемпотентности резервации.
-- flight_id ссылается на flights.id в БД Flight Service, поэтому внешнего ключа нет.
-- total_price — снимок seat_count * цена места на момент бронирования. Цена места отдельно
-- не хранится: она выводится из total_price / seat_count, и хранение обеих нарушило бы 3NF.
CREATE TABLE bookings (
    id              UUID PRIMARY KEY,
    user_id         VARCHAR(64)    NOT NULL CHECK (user_id ~ '^[A-Za-z0-9_.@-]{1,64}$'),
    flight_id       UUID           NOT NULL,
    passenger_name  VARCHAR(200)   NOT NULL CHECK (btrim(passenger_name) <> ''),
    passenger_email VARCHAR(254)   NOT NULL CHECK (passenger_email ~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'),
    seat_count      INTEGER        NOT NULL CHECK (seat_count > 0),
    total_price     NUMERIC(15, 2) NOT NULL CHECK (total_price > 0),
    currency        CHAR(3)        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status          booking_status NOT NULL DEFAULT 'CONFIRMED',
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- GET /bookings?user_id=X: выборка по пользователю, новые сверху.
CREATE INDEX idx_bookings_user_id_created_at ON bookings (user_id, created_at DESC);

CREATE TRIGGER bookings_set_updated_at
    BEFORE UPDATE ON bookings
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
