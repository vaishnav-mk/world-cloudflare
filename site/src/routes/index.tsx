import { createFileRoute } from '@tanstack/react-router';
import { DocPage } from '../components/doc-page';
import { docs } from '../content';

export const Route = createFileRoute('/')({ component: () => <DocPage doc={docs[0]} /> });
