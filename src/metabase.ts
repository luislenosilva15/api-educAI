import axios from 'axios';

const metabaseClient = axios.create({
  baseURL: process.env.METABASE_URL,
  headers: {
    'x-api-key': process.env.METABASE_TOKEN,
    'Content-Type': 'application/json',
  },
});

export async function getDatabases() {
  const { data } = await metabaseClient.get('/api/database');
  return data;
}

export async function getCollections() {
  const { data } = await metabaseClient.get('/api/collection');
  return data;
}

export async function getCardData(cardId: number) {
  const { data } = await metabaseClient.post(`/api/card/${cardId}/query/json`);
  return data;
}
