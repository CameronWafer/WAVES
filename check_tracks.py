import pandas as pd

df = pd.read_csv('C:/Users/HELIOS-300/Downloads/ACT24_behposture_event(in).csv')
df = df[df['Event_Type'] == 'State start']

domain_prefixes = {'sl-', 'pc-', 'ha-', 'ca-', 'wrk-', 'edu-', 'org-', 'pur-', 'eat-', 'les-', 'ex-', 'trav-', 'other-'}
posture_prefixes = {'sb-', 'la-', 'wa-', 'sp-'}

def classify(b):
    if pd.isna(b):
        return 'other'
    norm = str(b).strip().lower()
    if any(norm.startswith(p) for p in domain_prefixes):
        return 'activity'
    if any(norm.startswith(p) for p in posture_prefixes):
        return 'posture'
    return 'other'

df['track'] = df['Behavior'].apply(classify)

print('Track counts across all events:')
print(df['track'].value_counts())
print()

# Check first event per observation
df_sorted = df.sort_values(['Observation', 'Time_Relative_sf'])
first_events = df_sorted.groupby('Observation').first()

print('First event type per observation:')
print(first_events['track'].value_counts())
print()

# Check observations that start with activity-only
activity_start_obs = first_events[first_events['track'] == 'activity'].index.tolist()[:5]
print(f'Sample observations starting with activity (first 5): {activity_start_obs}')

# For one sample, show all its events
if activity_start_obs:
    sample_obs = activity_start_obs[0]
    print(f'\nAll events for {sample_obs}:')
    obs_events = df_sorted[df_sorted['Observation'] == sample_obs][['Time_Relative_sf', 'Behavior', 'track', 'Modifier_2']].head(10)
    print(obs_events)

