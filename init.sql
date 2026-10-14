CREATE TABLE articles (
    id         serial PRIMARY KEY,
    title      text,
    body       text,
    updated_at timestamptz
);
