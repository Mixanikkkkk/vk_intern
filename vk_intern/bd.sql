DROP DATABASE IF EXISTS intern_vk_db;


CREATE DATABASE intern_vk_db OWNER postgres;

\c intern_vk_db postgres;

CREATE TABLE IF NOT EXISTS users_by_posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT,
    body TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS top_users_by_posts (
    user_id INTEGER PRIMARY KEY,
    post_count INTEGER NOT NULL,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);