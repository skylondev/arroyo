import '@mantine/core/styles.css';
import 'mantine-react-table/styles.css';

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { MantineProvider } from '@mantine/core';
import App from './App.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <MantineProvider theme={{
      primaryColor: "teal",
    }} defaultColorScheme="dark">
      <App />
    </MantineProvider>
  </StrictMode>,
)
