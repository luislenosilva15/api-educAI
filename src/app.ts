import 'dotenv/config';
import express from 'express';
import metabaseRoutes from './routes/metabase.routes';

const app = express();

app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

app.use('/api', metabaseRoutes);

export default app;
