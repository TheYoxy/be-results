import { Athlete, Organization, PrismaClient } from '@prisma/client';
import axios from 'axios';
import debug from 'debug';
import qs from 'qs';
import 'dotenv';

const log = debug('app:prisma:seed');
const prisma = new PrismaClient();
const data = qs.stringify({
  'client_id': process.env.REMOTE_CLIENT_ID,
  'client_secret': process.env.REMOTE_CLIENT_SECRET,
  'scope': 'openid',
  'grant_type': 'client_credentials',
});
const config = {
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

async function getToken(): Promise<string> {
  const logger = log.extend('getToken');

  if (!process.env.REMOTE_AUTH_URL) {
    throw new Error('REMOTE_AUTH_URL is not defined');
  }

  try {
    logger('Getting token from remote');
    const response = await axios.post<{ access_token: string }>(process.env.REMOTE_AUTH_URL, data, config);
    return response.data.access_token;
  } catch (e) {
    console.error('An error occurred while getting the token: ', e);
    throw e;
  }
}

const chunkSize = 1000;

async function importOrgs(): Promise<void> {
  const token = await getToken();

  log('fetching organizations');
  const { data: fetchedOrganisations } = await axios.get<Organization[]>(
    `${process.env.REMOTE_API_URL}/api/organization/all`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );
  log('found %d organisations', fetchedOrganisations.length);

  for (let current = 0; current < fetchedOrganisations.length; current += chunkSize) {
    const absoluteCurrent: number = current / chunkSize;
    const absoluteTotal: number = fetchedOrganisations.length / chunkSize;
    log('saving %d/%d organisations', absoluteCurrent, absoluteTotal);
    const chunk = fetchedOrganisations.slice(current, current + chunkSize);
    const orgs = chunk.map((organization) => ({
      id: organization.id,
      fed_number: organization.fed_number,
      type: organization.type,
      name: organization.name,
      abbr: organization.abbr,
      alias: organization.alias,
      location: organization.location,
      federation: organization.federation,
      contact_person: organization.contact_person,
      contact_email: organization.contact_email,
      contact_phone1: organization.contact_phone1,
      contact_phone2: organization.contact_phone2,
    }));
    const { count: insertedOrgs } = await prisma.organization.createMany({ data: orgs, skipDuplicates: true });

    log.extend(absoluteCurrent.toString())('inserted %d organizations', insertedOrgs);
  }
}

async function importAthletes(): Promise<void> {
  const token = await getToken();
  log('getting athletes');
  const { data: fetchedAthlete } = await axios.get<Athlete[]>(
    `${process.env.REMOTE_API_URL}/api/athlete/all`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );


  for (let current = 0; current < fetchedAthlete.length; current += chunkSize) {
    const absoluteCurrent: number = (current) / chunkSize;
    const absoluteTotal: number = fetchedAthlete.length / chunkSize;
    log('saving %d/%d athletes', absoluteCurrent, absoluteTotal);
    const chunk = fetchedAthlete.slice(current, current + chunkSize);
    const athletes = chunk.map((athlete) => ({
      id: athlete.id,
      lastname: athlete.lastname,
      firstname: athlete.firstname,
      birthdate: athlete.birthdate,
      liveId: athlete.liveId,
      dossard: athlete.dossard,
      gender: athlete.gender,
      nationality: athlete.nationality,
      organizationId: athlete.organizationId,
    }));
    const { count: insertedAthletes } = await prisma.athlete.createMany({ data: athletes, skipDuplicates: true });

    log.extend(absoluteCurrent.toString())('inserted %d athletes', insertedAthletes);
  }
}

async function main() {
  await importOrgs();
  await importAthletes();
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
