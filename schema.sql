-- =============================================================================
-- Stellar Analysis Schema
-- Medallion architecture: bronze (raw) -> silver (typed/parsed) -> gold (stars)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE: Raw ingest, all columns TEXT to match CSV source exactly
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hipparcos_bronze (
    recno       TEXT,
    hip         TEXT,
    proxy       TEXT,
    rahms       TEXT,
    dedms       TEXT,
    vmag        TEXT,
    varflag     TEXT,
    r_vmag      TEXT,
    raicrs      TEXT,
    deicrs      TEXT,
    astroref    TEXT,
    plx         TEXT,
    pmra        TEXT,
    pmde        TEXT,
    e_raicrs    TEXT,
    e_deicrs    TEXT,
    e_plx       TEXT,
    e_pmra      TEXT,
    e_pmde      TEXT,
    "de:ra"     TEXT,
    "plx:ra"    TEXT,
    "plx:de"    TEXT,
    "pmra:ra"   TEXT,
    "pmra:de"   TEXT,
    "pmra:plx"  TEXT,
    "pmde:ra"   TEXT,
    "pmde:de"   TEXT,
    "pmde:plx"  TEXT,
    "pmde:pmra" TEXT,
    f1          TEXT,
    f2          TEXT,
    btmag       TEXT,
    e_btmag     TEXT,
    vtmag       TEXT,
    e_vtmag     TEXT,
    m_btmag     TEXT,
    b_v         TEXT,
    e_b_v       TEXT,
    r_b_v       TEXT,
    v_i         TEXT,
    e_v_i       TEXT,
    r_v_i       TEXT,
    combmag     TEXT,
    hpmag       TEXT,
    e_hpmag     TEXT,
    hpscat      TEXT,
    o_hpmag     TEXT,
    m_hpmag     TEXT,
    hpmax       TEXT,
    hpmin       TEXT,
    period      TEXT,
    hvartype    TEXT,
    morevar     TEXT,
    morephoto   TEXT,
    ccdm        TEXT,
    n_ccdm      TEXT,
    nsys        TEXT,
    ncomp       TEXT,
    multflag    TEXT,
    source      TEXT,
    qual        TEXT,
    m_hip       TEXT,
    theta       TEXT,
    rho         TEXT,
    e_rho       TEXT,
    dhp         TEXT,
    e_dhp       TEXT,
    survey      TEXT,
    chart       TEXT,
    notes       TEXT,
    hd          TEXT,
    bd          TEXT,
    cod         TEXT,
    cpd         TEXT,
    "(v_i)red"  TEXT,
    sptype      TEXT,
    r_sptype    TEXT,
    _ra_icrs    TEXT,
    _de_icrs    TEXT
);

CREATE TABLE IF NOT EXISTS woolley_bronze (
    recno       TEXT,
    woolley     TEXT,
    m_woolley   TEXT,
    plx         TEXT,
    e_plx       TEXT,
    n_plx       TEXT,
    pmra        TEXT,
    pmde        TEXT,
    rvel        TEXT,
    n_rvel      TEXT,
    u           TEXT,
    v           TEXT,
    w           TEXT,
    gcdist      TEXT,
    e           TEXT,
    i           TEXT,
    lc_code     TEXT,
    sptype      TEXT,
    r_sptype    TEXT,
    mag         TEXT,
    n_mag       TEXT,
    b_v         TEXT,
    u_b         TEXT,
    mv          TEXT,
    rab1950     TEXT,
    deb1950     TEXT,
    gctp        TEXT,
    hd          TEXT,
    dm          TEXT,
    gcrv        TEXT,
    pm_name     TEXT,
    hr          TEXT,
    vys         TEXT,
    remark1     TEXT,
    remark2     TEXT,
    _ra_icrs    TEXT,
    _de_icrs    TEXT
);

-- -----------------------------------------------------------------------------
-- SILVER: Typed and parsed columns, one row per observed star
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hipparcos_silver (
    ccdm        TEXT,               -- Double/multiple star system identifier
    plx         REAL,               -- Trigonometric parallax (mas)
    e_plx       REAL,               -- Standard error of parallax (mas)
    vmag        REAL,               -- Johnson V magnitude
    b_v         REAL,               -- B-V colour index
    e_b_v       REAL,               -- Standard error of B-V
    sptype      TEXT,               -- MK spectral type (raw)
    sp_class    TEXT,               -- Spectral class letter (O/B/A/F/G/K/M), parsed
    lum_class   TEXT,               -- Luminosity class (I/II/III/IV/V/VI/VII), parsed
    src         TEXT DEFAULT 'hipparcos'
);

CREATE INDEX IF NOT EXISTS idx_hip_silver_sp    ON hipparcos_silver (sp_class);
CREATE INDEX IF NOT EXISTS idx_hip_silver_lum   ON hipparcos_silver (lum_class);
CREATE INDEX IF NOT EXISTS idx_hip_silver_plx   ON hipparcos_silver (sp_class, plx DESC);

CREATE TABLE IF NOT EXISTS woolley_silver (
    recno       TEXT,               -- Record number (catalog identifier)
    plx         REAL,               -- Trigonometric parallax (mas)
    e_plx       REAL,               -- Standard error of parallax (mas)
    b_v         REAL,               -- B-V colour index
    mv          REAL,               -- Absolute visual magnitude (pre-computed in catalog)
    sptype      TEXT,               -- MK spectral type (raw)
    sp_class    TEXT,               -- Spectral class letter, parsed
    lum_class   TEXT,               -- Luminosity class, parsed/mapped from lc_code
    src         TEXT DEFAULT 'woolley'
);

CREATE INDEX IF NOT EXISTS idx_wly_silver_sp    ON woolley_silver (sp_class);
CREATE INDEX IF NOT EXISTS idx_wly_silver_lum   ON woolley_silver (lum_class);
CREATE INDEX IF NOT EXISTS idx_wly_silver_plx   ON woolley_silver (sp_class, plx DESC);

-- -----------------------------------------------------------------------------
-- GOLD: Unified analytical table, one row per observation with derived fields
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stars (
    dist_pc     REAL NOT NULL,      -- Distance in parsecs  (= 1000 / plx)
    abs_mag     REAL NOT NULL,      -- Absolute visual magnitude (distance-corrected)
    b_v         REAL NOT NULL,      -- B-V colour index (proxy for surface temperature)
    sp_class    TEXT NOT NULL,      -- Spectral class  (O B A F G K M)
    lum_class   TEXT NOT NULL,      -- Luminosity class (I II III IV V VI VII)
    src         TEXT NOT NULL       -- Source catalog ('hipparcos' | 'woolley')
);

CREATE INDEX IF NOT EXISTS idx_stars_sp      ON stars (sp_class);
CREATE INDEX IF NOT EXISTS idx_stars_lum     ON stars (lum_class);
CREATE INDEX IF NOT EXISTS idx_stars_sp_dist ON stars (sp_class,  dist_pc ASC);
CREATE INDEX IF NOT EXISTS idx_stars_lm_dist ON stars (lum_class, dist_pc ASC);
