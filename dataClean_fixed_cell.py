# Behavior ACT data cleaning - FIXED VERSION
# Separates activity and posture tracks to preserve simultaneous events at same timestamp

# quick log data cleaning
log_df['date'] = pd.to_datetime({
    'year': pd.to_numeric(log_df['start_year'], errors='coerce'),
    'month': pd.to_numeric(log_df['start_month'], errors='coerce'),
    'day': pd.to_numeric(log_df['start_day'], errors='coerce'),
}, errors='coerce').dt.strftime('%#m/%#d/%Y')

log_df.drop(columns=["start_month", "start_day", "start_year"], inplace=True)
log_df2 = log_df.loc[:, ["id", "do", "date", "start_time"]].copy()

# Convert log_df2 start_time to 24-hour HH:MM:SS
s = log_df2['start_time'].astype(str).str.strip()

# Support both with and without seconds
_dt1 = pd.to_datetime(s, format='%I:%M:%S %p', errors='coerce')
_dt2 = pd.to_datetime(s, format='%I:%M %p', errors='coerce')

log_df2.loc[:, 'start_time'] = _dt1.fillna(_dt2).dt.strftime('%H:%M:%S')
log_df2.loc[:, 'date_time'] = log_df2['date'].astype(str).str.strip() + ' ' + log_df2['start_time'].astype(str).str.strip()

log_df2.rename(columns={"start_time" : "time", "do" : "obs"}, inplace=True)
log_df2 = log_df2.drop(columns=["time", "date_time"])


# Start behavior cleaning
# a) why does unnamed 17 and 18 exist? no one knows :)
behav_act_df1 = behav_act_df.drop(columns=["Date_Time_Absolute_dmy_hmsf", 
"Date_dmy", 
"Time_Absolute_hms", 
"Time_Absolute_f", 
"Unnamed: 17", 
"Unnamed: 18",
"Event_Log"])

# add "id" and "do" style ID's from LOG into ACT behavior file so we can join
def add_id_do_split(df, source_col='Observation', id_col='id', do_col='do', inplace=True):
    parts = df[source_col].str.split('_', expand=True)
    id_series = pd.to_numeric(parts[1], errors='coerce').astype('Int64')
    do_series = pd.to_numeric(parts[2], errors='coerce').astype('Int64')
    if inplace:
        df[id_col] = id_series
        df[do_col] = do_series
        return df
    out = df.copy()
    out[id_col] = id_series
    out[do_col] = do_series
    return out

# add a column from LOG onto Behavior based on "id" and "do"
def add_col_from_other_df_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_keys: list,
    right_keys: list,
    right_value_col: str,
    new_col_name: str | None = None,
    how: str = 'left',
    validate: str = 'many_to_one'
) -> pd.DataFrame:
    """
    Add a single column from `right` to `left` by joining on two (or more) key columns.
    """
    if new_col_name is None:
        new_col_name = right_value_col

    right_subset = right[right_keys + [right_value_col]].rename(
        columns={right_value_col: new_col_name}
    )
    merged = left.merge(
        right_subset,
        how=how,
        left_on=left_keys,
        right_on=right_keys,
        validate=validate
    )
    return merged

behav_act_df2 = add_id_do_split(behav_act_df1)

behav_act_df3 = add_col_from_other_df_merge(
    left=behav_act_df2,                 # main df (to put columns into)
    right=log_df,               # other df (to pull the columns from)
    left_keys=['id', 'do'],
    right_keys=['id', 'do'],
    right_value_col='start_time',      # column from df_right to bring over        # optional rename
    how='left',
    validate='many_to_one'        # set 'one_to_one' to enforce uniqueness if applicable (not here yet really)
)

series_temp = behav_act_df3.pop("start_time")
behav_act_df3.insert(0, "start_time", series_temp)

behav_act_df4 = behav_act_df3.drop(index=behav_act_df3.index[behav_act_df3["Event_Type"] != "State start"])

# parse start_time (supports "8:20:19 AM" and "8:20 AM")
s = behav_act_df4['start_time'].astype(str).str.strip()
dt1 = pd.to_datetime(s, format='%I:%M:%S %p', errors='coerce')
dt2 = pd.to_datetime(s, format='%I:%M %p', errors='coerce')
behav_act_df4['start_time_dt'] = dt1.fillna(dt2)

# parse Time_Relative_hmsf (supports "HH:MM:SS(.f)", "MM:SS(.f)", "SS(.f)")
r = behav_act_df4['Time_Relative_hmsf'].astype(str).str.strip()
r = r.str.replace(',', '.', regex=False).str.replace(';', '.', regex=False)

td = pd.Series(pd.NaT, index=r.index, dtype='timedelta64[ns]')
mask_hms = r.str.count(':') == 2
mask_ms  = r.str.count(':') == 1
mask_sec = r.str.fullmatch(r'\d+(\.\d+)?')
mask_blank = r.eq('') | r.str.lower().isin(['nan', 'none'])

td.loc[mask_hms] = pd.to_timedelta(r[mask_hms], errors='coerce')
td.loc[mask_ms]  = pd.to_timedelta('00:' + r[mask_ms], errors='coerce')  # prefix hours
td.loc[mask_sec] = pd.to_timedelta(r[mask_sec].astype(float), unit='s')
td.loc[mask_blank] = pd.NaT

behav_act_df4['time_relative_td'] = td

# sum to produce the new start time
behav_act_df5 = behav_act_df4.copy()
behav_act_df5['start_time_new'] = behav_act_df5['start_time_dt'] + behav_act_df5['time_relative_td']

# time-only display strings (no date)
behav_act_df5['start_time_str'] = behav_act_df5['start_time_dt'].dt.strftime('%I:%M:%S %p')
behav_act_df5['start_time_new_str'] = behav_act_df5['start_time_new'].dt.strftime('%I:%M:%S %p')

# drop intermediates, rename, and position between the first two columns
drop_cols = [c for c in ['start_time_dt','time_relative_td','start_time_new','start_time_str'] if c in behav_act_df5.columns]
behav_act_df5 = behav_act_df5.drop(columns=drop_cols)

behav_act_df5 = behav_act_df5.rename(columns={'start_time_new_str': 'start_time_new'})

first_cols = ['start_time', 'start_time_new', 'Time_Relative_hmsf']
other_cols = [c for c in behav_act_df5.columns if c not in first_cols]
behav_act_df5 = behav_act_df5[first_cols + other_cols]


# ============================================================================
# NEW APPROACH: Separate activity and posture tracks before expansion
# ============================================================================

# --- Helper functions (shared) ---
def _parse_hms_to_seconds(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(',', '.', regex=False).str.replace(';', '.', regex=False)

    td = pd.Series(pd.NaT, index=s.index, dtype='timedelta64[ns]')
    mask_hms = s.str.count(':') == 2           # H:M:S(.f)
    mask_ms  = s.str.count(':') == 1           # M:S(.f)
    mask_sec = s.str.fullmatch(r'\d+(\.\d+)?') # seconds only
    mask_blank = s.eq('') | r.str.lower().isin(['nan', 'none'])

    td.loc[mask_hms] = pd.to_timedelta(s[mask_hms], errors='coerce')
    td.loc[mask_ms]  = pd.to_timedelta('00:' + s[mask_ms], errors='coerce')
    if mask_sec.any():
        td.loc[mask_sec] = pd.to_timedelta(s[mask_sec].astype(float), unit='s')
    td.loc[mask_blank] = pd.NaT

    return td.dt.total_seconds()

def _format_hms(seconds_float: float, decimals: int = 0) -> str:
    if pd.isna(seconds_float):
        return np.nan
    scale = 10 ** decimals
    total_units = int(round(seconds_float * scale))
    secs = total_units // scale
    frac_units = total_units % scale
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    if decimals == 0:
        return f'{h:02d}:{m:02d}:{s:02d}'
    return f'{h:02d}:{m:02d}:{s:02d}.{frac_units:0{decimals}d}'

# Normalization for classification
_def_ws_re = re.compile(r"\s+")

def _normalize_behavior(value: object) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).strip().lower()
    s = s.replace('–', '-').replace('—', '-')
    s = _def_ws_re.sub(' ', s)
    return s

# Classify behavior as domain activity or posture
_domain_prefixes = {
    'sl-', 'pc-', 'ha-', 'ca-', 'wrk-', 'edu-', 'org-', 'pur-', 'eat-', 'les-', 'ex-', 'trav-', 'other-'
}
_posture_prefixes = {
    'sb-', 'la-', 'wa-', 'sp-'
}

def _classify_behavior(behavior_val):
    """Returns 'activity', 'posture', or 'other'"""
    norm = _normalize_behavior(behavior_val)
    if not norm:
        return 'other'
    # Check domain prefixes
    for prefix in _domain_prefixes:
        if norm.startswith(prefix):
            return 'activity'
    # Check posture prefixes
    for prefix in _posture_prefixes:
        if norm.startswith(prefix):
            return 'posture'
    # Handle special cases
    if norm in {'private/not coded', 'start posture', 'start behavior'}:
        return 'other'
    return 'other'

df = behav_act_df5.copy()
df['_seconds'] = _parse_hms_to_seconds(df['Time_Relative_hms'])
df = df.sort_values(['Observation', '_seconds'], kind='mergesort')

# Classify each row
df['_track'] = df['Behavior'].apply(_classify_behavior)

# Split into activity and posture dataframes
activity_df = df[df['_track'] == 'activity'].copy()
posture_df = df[df['_track'] == 'posture'].copy()

print(f"Activity events: {len(activity_df)}, Posture events: {len(posture_df)}, Other: {(df['_track'] == 'other').sum()}")

# --- Expand activity track to per-second ---
def expand_track_to_seconds(track_df, track_name='track'):
    """Expand a track (activity or posture) to per-second resolution"""
    out_groups = []
    
    for obs_value, g in track_df.groupby('Observation', sort=False):
        g = g.copy()
        g = g[~g['_seconds'].isna()]
        if g.empty:
            continue
        
        g['_event_second'] = np.floor(g['_seconds']).astype(int)
        
        # Keep last within each second
        g_last = (
            g.sort_values(['_event_second', '_seconds'], kind='mergesort')
             .drop_duplicates(subset=['_event_second'], keep='last')
        )
        
        min_s = float(g['_seconds'].min())
        max_s = float(g['_seconds'].max())
        
        if np.isclose(min_s, 0.0):
            start_second = 0
            flag_first = False
        else:
            start_second = int(np.ceil(min_s))
            flag_first = True
        
        end_second = int(np.floor(max_s))
        if end_second < start_second:
            end_second = start_second
        
        seconds_grid = np.arange(start_second, end_second + 1, dtype=int)
        
        base = g_last.set_index('_event_second').sort_index()
        first_index_second = int(np.floor(min_s))
        full_index = np.arange(first_index_second, end_second + 1, dtype=int)
        aligned = base.reindex(full_index).ffill()
        
        take = aligned.loc[seconds_grid].copy()
        take.reset_index(drop=False, inplace=True)
        take.rename(columns={'_event_second': '_second'}, inplace=True)
        
        # Time strings
        time_strings = [_format_hms(s, decimals=0) for s in seconds_grid]
        if flag_first and len(time_strings) > 0:
            flagged = min_s + 0.01
            time_strings[0] = _format_hms(flagged, decimals=2)
        
        take['Time_Relative_hms_new'] = time_strings
        
        out_groups.append(take)
    
    if not out_groups:
        return pd.DataFrame()
    
    result = pd.concat(out_groups, axis=0, ignore_index=True)
    return result

# Expand both tracks
activity_expanded = expand_track_to_seconds(activity_df, 'activity')
posture_expanded = expand_track_to_seconds(posture_df, 'posture')

print(f"Activity expanded: {len(activity_expanded)}, Posture expanded: {len(posture_expanded)}")

# --- Merge activity and posture on (Observation, second) ---
# Keep columns needed from each track (including modifiers from BOTH)
activity_cols_keep = ['Observation', '_second', 'Time_Relative_hms_new', 'Behavior', 'Modifier_1', 'Modifier_2', 'Modifier_3', 
                      'start_time_new', 'id', 'do']
posture_cols_keep = ['Observation', '_second', 'Behavior', 'Modifier_2']  # Modifier_2 for intensity

activity_subset = activity_expanded[activity_cols_keep].rename(columns={'Behavior': 'Behavior_activity', 
                                                                          'Modifier_1': 'Modifier_1_activity',
                                                                          'Modifier_2': 'Modifier_2_activity',
                                                                          'Modifier_3': 'Modifier_3'})
posture_subset = posture_expanded[posture_cols_keep].rename(columns={'Behavior': 'Behavior_posture',
                                                                       'Modifier_2': 'Modifier_2_posture'})

# Full outer merge to get all seconds from both tracks
merged = activity_subset.merge(
    posture_subset,
    on=['Observation', '_second'],
    how='outer',
    suffixes=('', '_posture')
)

# Fill observation metadata forward
merged = merged.sort_values(['Observation', '_second'], kind='mergesort')
for col in ['id', 'do', 'Time_Relative_hms_new', 'start_time_new']:
    if col in merged.columns:
        merged[col] = merged.groupby('Observation')[col].ffill().bfill()

# Combine Behaviors: use activity behavior for encoding activity_type, posture behavior for encoding posture
merged['Behavior'] = merged['Behavior_activity'].fillna(merged['Behavior_posture'])

# Combine Modifier_1 and Modifier_3 (activity-related modifiers)
merged['Modifier_1'] = merged['Modifier_1_activity']
merged['Modifier_3'] = merged['Modifier_3']

# Combine Modifier_2 (intensity): prefer posture track, fallback to activity track
merged['Modifier_2'] = merged['Modifier_2_posture'].fillna(merged['Modifier_2_activity'])

# Rename _second to rel_time for final output
merged['rel_time'] = merged['Time_Relative_hms_new']

behav_act_df_6 = merged.copy()

# Cleanup helper columns
for c in ['_seconds', '_second', '_track', 'Behavior_activity', 'Behavior_posture', 
          'Modifier_1_activity', 'Modifier_2_activity', 'Modifier_2_posture']:
    if c in behav_act_df_6.columns:
        behav_act_df_6 = behav_act_df_6.drop(columns=c)

print(f"Merged result: {len(behav_act_df_6)} rows")


# ============================================================================
# ENCODING: Activity and Posture (independent tracks, same as before)
# ============================================================================

behav_act_df_7 = behav_act_df_6.copy()

# Mapping from canonical Activity_Type to (activity_type, broad_domain, waves_domain)
_activity_type_to_meta = {
    'SL- Sleep': ('sleep', 'sleep', 'household_personal'),
    'PC- Groom, Health-Related': ('pc_groom', 'personal', 'household_personal'),
    'PC- Other Personal Care': ('pc_other', 'personal', 'household_personal'),
    'HA- Housework': ('ha_housework', 'household', 'household_personal'),
    'HA- Food Prep and Cleanup': ('ha_food', 'household', 'household_personal'),
    'HA- Interior Maintenance, Repair, & Decoration': ('ha_interior', 'maintenance_repair', 'household_personal'),
    'HA- Exterior Maintenance, Repair, & Decoration': ('ha_exterior', 'maintenance_repair', 'household_personal'),
    'HA- Lawn, Garden and Houseplants': ('ha_lawn', 'lawn_garden', 'household_personal'),
    'HA- Animals and Pets': ('ha_pets', 'household', 'household_personal'),
    'HA- Household Management/Other household activities': ('ha_other', 'household', 'household_personal'),
    'CA- Caring for and Helping Children': ('care_children', 'household', 'household_personal'),
    'CA- Caring for and Helping Adults': ('care_adults', 'household', 'household_personal'),
    'WRK- General**': ('work_general', 'work_education', 'work_education'),
    'WRK- Desk/Screen Based': ('work_screen', 'work_education', 'work_education'),
    'EDU- Taking Class, Research, Homework': ('edu_class', 'work_education', 'work_education'),
    'EDU- Extracurricular': ('edu_other', 'work_education', 'work_education'),
    'ORG- Church, Spiritual': ('com_church', 'purchase_other', 'purchase_other'),
    'Volunteer Work (ORG - Volunteer Work)': ('com_volunteer', 'purchase_other', 'purchase_other'),
    'PUR- Purchasing Goods and Services': ('com_purchase', 'purchase_other', 'purchase_other'),
    'EAT- Eating and Drinking, Waiting': ('ha_eat', 'personal', 'household_personal'),
    'LES- Socializing, Communicating, Non-Screen Based': ('les_social', 'leisure', 'leisure'),
    'LES- Screen-Based (TV, Video Game, Computer, Phone)': ('les_screen', 'Leisure_Screen', 'leisure'),
    'EX- Participating in Sport, Exercise or Recreation***': ('ex_sport', 'exercise', 'leisure'),
    'EX- Attending Sport, Exercise Recreation Event, or Performance': ('les_attend', 'leisure', 'leisure'),
    'TRAV- Passenger (Car/Truck/Motorcycle)': ('trav_pass', 'Trav_car', 'transportation'),
    'TRAV- Driver (Car/Truck/Motorcycle)': ('trav_drive', 'Trav_car', 'transportation'),
    'TRAV- Passenger (Bus, Train, Tram, Plane, Boat, Ship)': ('trav_pass', 'Trav_public', 'transportation'),
    'TRAV- Biking': ('trav_bike', 'active_transportation', 'transportation'),
    'TRAV-Walking': ('trav_walk', 'active_transportation', 'transportation'),
    'TRAV- General': ('trav_other', 'transportation', 'transportation'),
    'OTHER- Non-Codable (delete these rows from dataset)': ('non_codable', 'non_codable', 'non_codable'),
}

# Map raw Behavior values to canonical Activity_Type keys above
_alias_to_activity_type = {
    'sl- sleep': 'SL- Sleep',
    'pc- groom, health-related': 'PC- Groom, Health-Related',
    'pc- other personal care': 'PC- Other Personal Care',
    'ha- housework': 'HA- Housework',
    'ha- food prep and cleanup': 'HA- Food Prep and Cleanup',
    'ha- interior maintenance, repair, & decoration': 'HA- Interior Maintenance, Repair, & Decoration',
    'ha- exterior maintenance, repair, & decoration': 'HA- Exterior Maintenance, Repair, & Decoration',
    'ha- lawn, garden and houseplants': 'HA- Lawn, Garden and Houseplants',
    'ha- animals and pets': 'HA- Animals and Pets',
    'ha- household management/other household activities': 'HA- Household Management/Other household activities',
    'ca- caring for and helping children': 'CA- Caring for and Helping Children',
    'ca- caring for and helping adults': 'CA- Caring for and Helping Adults',
    'wrk- general': 'WRK- General**',
    'wrk- screen based': 'WRK- Desk/Screen Based',
    'edu- taking class, research, homework': 'EDU- Taking Class, Research, Homework',
    'edu- extracurricular': 'EDU- Extracurricular',
    'org- church, spiritual': 'ORG- Church, Spiritual',
    'org- volunteer': 'Volunteer Work (ORG - Volunteer Work)',
    'pur- purchasing goods and services': 'PUR- Purchasing Goods and Services',
    'eat- eating and drinking, waiting': 'EAT- Eating and Drinking, Waiting',
    'les- socializing, communicating, leisure time not screen': 'LES- Socializing, Communicating, Non-Screen Based',
    'les- screen based leisure time (tv, video game, computer)': 'LES- Screen-Based (TV, Video Game, Computer, Phone)',
    'les- screen-based (tv, video game, computer, phone)': 'LES- Screen-Based (TV, Video Game, Computer, Phone)',
    'ex- participating in sport, exercise or recreation': 'EX- Participating in Sport, Exercise or Recreation***',
    'ex- attending sport, recreational event, or performance': 'EX- Attending Sport, Exercise Recreation Event, or Performance',
    'trav- passenger (car/truck/motorcycle)': 'TRAV- Passenger (Car/Truck/Motorcycle)',
    'trav- driver (car/truck/motorcycle)': 'TRAV- Driver (Car/Truck/Motorcycle)',
    'trav- passenger (bus, train, tram, plane, boat, ship)': 'TRAV- Passenger (Bus, Train, Tram, Plane, Boat, Ship)',
    'trav- biking': 'TRAV- Biking',
    'trav- walking': 'TRAV-Walking',
    'trav-walking': 'TRAV-Walking',
    'trav- general': 'TRAV- General',
    'other- non codable': 'OTHER- Non-Codable (delete these rows from dataset)',
    'private/not coded': 'OTHER- Non-Codable (delete these rows from dataset)',
}

def _map_behavior_to_activity_type(value: object) -> str | None:
    s = _normalize_behavior(value)
    if not s:
        return None
    if s.startswith('les- screen'):
        return 'LES- Screen-Based (TV, Video Game, Computer, Phone)'
    if s.startswith('trav- passenger (bus'):
        return 'TRAV- Passenger (Bus, Train, Tram, Plane, Boat, Ship)'
    return _alias_to_activity_type.get(s)

# Build Activity_Type from Behavior_activity column (preserved from activity track)
if 'Behavior_activity' in behav_act_df_7.columns:
    behav_act_df_7['Activity_Type'] = behav_act_df_7['Behavior_activity'].apply(_map_behavior_to_activity_type)
else:
    # Fallback: classify on the fly from merged Behavior
    behav_act_df_7['Activity_Type'] = behav_act_df_7['Behavior'].apply(
        lambda b: _map_behavior_to_activity_type(b) if _classify_behavior(b) == 'activity' else None
    )

# EX modifier handling
if 'Modifier_1' in behav_act_df_7.columns:
    mask_ex = behav_act_df_7['Activity_Type'] == 'EX- Participating in Sport, Exercise or Recreation***'
    mask_m1 = behav_act_df_7['Modifier_1'].notna()
    mask_apply = mask_ex & mask_m1
    if mask_apply.any():
        mod1_norm = (
            behav_act_df_7.loc[mask_apply, 'Modifier_1']
            .astype(str).str.strip().str.lower()
            .str.replace(r'\s+', '-', regex=True).str.replace('/', '-')
        )
        behav_act_df_7.loc[mask_apply, 'Activity_Type'] = 'EX-' + mod1_norm

# work_type from Modifier_3
work_labels = {'WRK- General**', 'WRK- Desk/Screen Based'}
if 'Modifier_3' in behav_act_df_7.columns:
    def _mk_work_type(x):
        if pd.isna(x):
            return np.nan
        raw = str(x).strip()
        raw = re.sub(r'^\s*sp-\s*', '', raw, flags=re.IGNORECASE)
        s = re.sub(r"\s+", '_', raw.lower()).replace('/', '_')
        s = s.replace('hospiltality', 'hospitality')
        return f"work_{s}" if s else np.nan
    behav_act_df_7['work_type_raw'] = behav_act_df_7['Modifier_3'].apply(_mk_work_type)
else:
    behav_act_df_7['work_type_raw'] = np.nan

# Expand Activity_Type to three encoded columns
cols = ['activity_type', 'broad_domain', 'waves_domain']

def _activity_meta_lookup(activity_type: object):
    if isinstance(activity_type, str) and activity_type.startswith('EX-'):
        return ('ex_sport', 'exercise', 'leisure')
    return _activity_type_to_meta.get(activity_type)

behav_act_df_7[cols] = behav_act_df_7['Activity_Type'].map(_activity_meta_lookup).apply(
    lambda tpl: pd.Series(tpl if isinstance(tpl, tuple) else (np.nan, np.nan, np.nan))
)

# Detect grouping
if 'Observation' in behav_act_df_7.columns:
    _group_cols = ['Observation']
elif {'id','do'}.issubset(behav_act_df_7.columns):
    _group_cols = ['id','do']
else:
    _group_cols = None

# Forward-fill Activity_Type within observation
if _group_cols is not None:
    behav_act_df_7['Activity_Type'] = behav_act_df_7.groupby(_group_cols)['Activity_Type'].ffill()
    behav_act_df_7[cols] = behav_act_df_7['Activity_Type'].map(_activity_meta_lookup).apply(
        lambda tpl: pd.Series(tpl if isinstance(tpl, tuple) else (np.nan, np.nan, np.nan))
    )

# Posture encoding
def _map_posture_wbm_from_behavior(value: object) -> str | None:
    s = _normalize_behavior(value)
    if not s:
        return None
    if s.startswith('sb-sitting'):
        return 'sitting'
    if s.startswith('sb-lying') or s.startswith('sb- lying'):
        return 'lying'
    if s.startswith('la- kneeling'):
        return 'kneel_squat'
    if s == 'la- stretching':
        return 'stretch'
    if s == 'la- stand and move':
        return 'stand_move'
    if s == 'la- stand':
        return 'stand'
    if s in {'wa- walk', 'wa- walking', 'trav- walking', 'trav-walking'}:
        return 'walk'
    if s in {'wa-walk with load', 'wa- walk with load'}:
        return 'walk_load'
    if s == 'wa- ascend stairs':
        return 'ascend'
    if s == 'wa- descend stairs':
        return 'descend'
    if s == 'wa- running':
        return 'running'
    if s == 'sp- bike':
        return 'biking'
    if s in {'sp- other sport movement', 'sp- swing', 'sp -kick', 'sp- jump'}:
        return 'sport_move'
    if s == 'sp- muscle strengthening':
        return 'muscle_strength'
    if s == 'private/not coded':
        return 'not_coded'
    return None

_posture_meta = {
    'sitting': ('sedentary', 'sedentary'),
    'lying': ('sedentary', 'sedentary'),
    'kneel_squat': ('sedentary', 'mixed_move'),
    'stretch': ('sport', 'sport'),
    'stand': ('stand_move', 'mixed_move'),
    'stand_move': ('stand_move', 'mixed_move'),
    'walk': ('walk', 'walk'),
    'walk_load': ('mod_walk', 'walk'),
    'ascend': ('mod_walk', 'walk'),
    'descend': ('mod_walk', 'walk'),
    'running': ('running', 'running'),
    'biking': ('biking', 'biking'),
    'sport_move': ('sport', 'sport'),
    'muscle_strength': ('sport', 'sport'),
    'not_coded': ('not_coded', 'not_coded'),
}

# Build posture from merged Behavior column (picks up posture from either track)
behav_act_df_7['posture_wbm'] = behav_act_df_7['Behavior'].apply(_map_posture_wbm_from_behavior)

_broad_waves = behav_act_df_7['posture_wbm'].map(lambda k: _posture_meta.get(k, (np.nan, np.nan)))
behav_act_df_7[['posture_broad', 'posture_waves']] = pd.DataFrame(_broad_waves.tolist(), index=behav_act_df_7.index)

# Forward-fill posture within observation
if _group_cols is not None:
    for _c in ['posture_wbm', 'posture_broad', 'posture_waves']:
        behav_act_df_7[_c] = behav_act_df_7.groupby(_group_cols)[_c].ffill()

# waves_sedentary
def _waves_sed_vec(posture_wbm, activity_type):
    """Vectorized waves_sedentary computation"""
    result = pd.Series(index=posture_wbm.index, dtype='object')
    
    mask_sit = posture_wbm == 'sitting'
    mask_drive = activity_type.isin({'trav_drive', 'trav_pass'})
    result.loc[mask_sit & mask_drive] = 'sed_drive'
    result.loc[mask_sit & ~mask_drive] = 'sedentary'
    
    mask_lying_kneel = posture_wbm.isin({'lying', 'kneel_squat'})
    result.loc[mask_lying_kneel] = 'sedentary'
    
    mask_active = posture_wbm.notna() & ~mask_sit & ~mask_lying_kneel
    result.loc[mask_active] = 'active'
    
    return result

behav_act_df_7['waves_sedentary'] = _waves_sed_vec(behav_act_df_7['posture_wbm'], behav_act_df_7['activity_type'])

# Intensity encoding
def _posture_intensity(value: object) -> str | None:
    s = _normalize_behavior(value)
    if not s:
        return None
    if s.startswith('sb-sitting') or s.startswith('sb-lying') or s.startswith('sb- lying') or s.startswith('la- kneeling'):
        return 'sedentary'
    if s in {'la- stand', 'la- stand and move', 'la- stretching'}:
        return 'light'
    return None

behav_act_df_7['intensity'] = behav_act_df_7['Behavior'].apply(_posture_intensity)

# Fill from Modifier_2 only where intensity is still missing
if 'Modifier_2' in behav_act_df_7.columns:
    def _norm_intensity(m) -> str | None:
        if pd.isna(m):
            return None
        s = str(m).strip().lower()
        if not s:
            return None
        if s.startswith('vig'):
            return 'vigorous'
        if s.startswith('mod'):
            return 'moderate'
        if s == 'light':
            return 'light'
        if s == 'sedentary':
            return 'sedentary'
        return None
    _mask_missing = behav_act_df_7['intensity'].isna()
    behav_act_df_7.loc[_mask_missing, 'intensity'] = behav_act_df_7.loc[_mask_missing, 'Modifier_2'].apply(_norm_intensity)

# Forward-fill intensity within observation
if _group_cols is not None:
    behav_act_df_7['intensity'] = behav_act_df_7.groupby(_group_cols)['intensity'].ffill()

# waves_intensity
behav_act_df_7['waves_intensity'] = behav_act_df_7['intensity'].map(lambda x: 'mvpa' if x in {'moderate', 'vigorous'} else x)

# Finalize work_type
if 'work_type_raw' in behav_act_df_7.columns:
    if _group_cols is not None:
        behav_act_df_7['work_type_raw'] = behav_act_df_7.groupby(_group_cols)['work_type_raw'].ffill()
    behav_act_df_7['work_type'] = np.where(
        behav_act_df_7['Activity_Type'].isin(work_labels),
        behav_act_df_7['work_type_raw'],
        np.nan,
    )
    behav_act_df_7 = behav_act_df_7.drop(columns=['work_type_raw'])

# Drop non-codable
_non_codable_mask = (
    behav_act_df_7['Activity_Type'] == 'OTHER- Non-Codable (delete these rows from dataset)'
) | (
    behav_act_df_7['Behavior'].astype(str).str.strip().str.lower().isin(['private/not coded'])
)
behav_act_df_7 = behav_act_df_7.loc[~_non_codable_mask].copy()

print(f"After encoding, behav_act_df_7 shape: {behav_act_df_7.shape}")
print(f"activity_type NaN: {behav_act_df_7['activity_type'].isna().sum()}")
print(f"posture_wbm NaN: {behav_act_df_7['posture_wbm'].isna().sum()}")

# Stabilize both tracks with ffill+bfill
if _group_cols is not None:
    # Activity track
    _before_act = behav_act_df_7['Activity_Type'].isna().sum()
    ff_act = behav_act_df_7.groupby(_group_cols, sort=False)['Activity_Type'].ffill()
    bf_act = behav_act_df_7.groupby(_group_cols, sort=False)['Activity_Type'].bfill()
    behav_act_df_7['Activity_Type'] = ff_act.fillna(bf_act)
    
    # Recompute activity meta
    behav_act_df_7[cols] = behav_act_df_7['Activity_Type'].map(_activity_meta_lookup).apply(
        lambda tpl: pd.Series(tpl if isinstance(tpl, tuple) else (np.nan, np.nan, np.nan))
    )
    _after_act = behav_act_df_7['Activity_Type'].isna().sum()
    
    # Posture track
    _before_pos = behav_act_df_7['posture_wbm'].isna().sum()
    ff_pos = behav_act_df_7.groupby(_group_cols, sort=False)['posture_wbm'].ffill()
    bf_pos = behav_act_df_7.groupby(_group_cols, sort=False)['posture_wbm'].bfill()
    behav_act_df_7['posture_wbm'] = ff_pos.fillna(bf_pos)
    
    # Recompute posture meta
    _pw = behav_act_df_7['posture_wbm'].map(lambda k: _posture_meta.get(k, (np.nan, np.nan)))
    behav_act_df_7[['posture_broad', 'posture_waves']] = pd.DataFrame(_pw.tolist(), index=behav_act_df_7.index)
    _after_pos = behav_act_df_7['posture_wbm'].isna().sum()
    
    # Recompute waves_sedentary
    behav_act_df_7['waves_sedentary'] = _waves_sed_vec(behav_act_df_7['posture_wbm'], behav_act_df_7['activity_type'])
    
    print(f"Stabilization: activity_type {_before_act} -> {_after_act}, posture_wbm {_before_pos} -> {_after_pos}")

# Build behav_copy for final output
behav_copy = behav_act_df_7[["id", "do", "rel_time", "activity_type", "posture_waves", "intensity", "start_time_new"]]
behav_copy = behav_copy.rename(columns={"do" : "obs"})
behav_copy.head()