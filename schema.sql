CREATE TABLE IF NOT EXISTS videos (
    id VARCHAR(64) PRIMARY KEY,
    creator_id VARCHAR(64) NOT NULL,
    video_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    views_count BIGINT NOT NULL,
    likes_count BIGINT NOT NULL,
    comments_count BIGINT NOT NULL,
    reports_count BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(64) REFERENCES videos (id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    views_count BIGINT NOT NULL,
    delta_views_count BIGINT NOT NULL, 
    likes_count BIGINT NOT NULL,
    delta_likes_count BIGINT NOT NULL,
    comments_count BIGINT NOT NULL,
    delta_comments_count BIGINT NOT NULL,
    reports_count BIGINT NOT NULL,
    delta_reports_count BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    
);

CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots (video_id);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots (created_at);