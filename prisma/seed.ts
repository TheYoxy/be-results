import { Athlete, Organization, PrismaClient } from '@prisma/client';
import axios from 'axios';
import debug from 'debug';
import 'dotenv';
import ora from 'ora';
import * as qs from 'qs';

const log = debug('app:prisma:seed');
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

async function importOrgs(prisma: PrismaClient): Promise<void> {
  const token = await getToken();

  log('fetching organizations');
  const fetch = ora('fetching organizations').start();
  const { data: fetchedOrganisations } = await axios.get<Organization[]>(
    `${process.env.REMOTE_API_URL}/api/organization/all`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );
  log('found %d organisations', fetchedOrganisations.length);
  fetch.succeed('fetched ' + fetchedOrganisations.length + ' organisations');

  const insert = ora('inserting organizations').start();
  for (let current = 0; current < fetchedOrganisations.length; current += chunkSize) {
    const absoluteCurrent: number = Math.round(current / chunkSize);
    const absoluteTotal: number = Math.round(fetchedOrganisations.length / chunkSize);
    log('saving %d/%d organisations', absoluteCurrent, absoluteTotal);
    const chunk = fetchedOrganisations.slice(current, current + chunkSize);
    const orgs = chunk.map((organization) => ({
      id: organization.id ?? undefined,
      fed_number: organization.fed_number ?? undefined,
      type: organization.type ?? undefined,
      name: organization.name ?? undefined,
      abbr: organization.abbr ?? undefined,
      alias: organization.alias ?? undefined,
      location: organization.location ?? undefined,
      federation: organization.federation ?? undefined,
      contact_person: organization.contact_person ?? undefined,
      contact_email: organization.contact_email ?? undefined,
      contact_phone1: organization.contact_phone1 ?? undefined,
      contact_phone2: organization.contact_phone2 ?? undefined,
    }));
    const { count: insertedOrgs } = await prisma.organization.createMany({ data: orgs, skipDuplicates: true });

    log.extend(absoluteCurrent.toString())('inserted %d organizations', insertedOrgs);
    insert.text = `inserted ${absoluteCurrent}/${absoluteTotal} organizations`;
  }
  insert.succeed('inserted ' + fetchedOrganisations.length + ' organizations');
}

async function importAthletes(prisma: PrismaClient): Promise<void> {
  const token = await getToken();
  log('getting athletes');

  const fetch = ora('fetching athletes').start();
  const { data: fetchedAthlete } = await axios.get<Athlete[]>(
    `${process.env.REMOTE_API_URL}/api/athlete/all`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );
  fetch.succeed('fetched ' + fetchedAthlete.length + ' athletes');


  const insert = ora('inserting athletes').start();
  for (let current = 0; current < fetchedAthlete.length; current += chunkSize) {
    const absoluteCurrent: number = Math.round(current / chunkSize);
    const absoluteTotal: number = Math.round(fetchedAthlete.length / chunkSize);
    log('saving %d/%d athletes', absoluteCurrent, absoluteTotal);
    const chunk = fetchedAthlete.slice(current, current + chunkSize);
    const athletes = chunk.map((athlete) => ({
      id: athlete.id ?? undefined,
      lastname: athlete.lastname ?? undefined,
      firstname: athlete.firstname ?? undefined,
      birthdate: athlete.birthdate ?? undefined,
      liveId: athlete.liveId ?? undefined,
      dossard: athlete.dossard ?? undefined,
      gender: athlete.gender ?? undefined,
      nationality: athlete.nationality ?? undefined,
      organizationId: athlete.organizationId ?? undefined,
    }));
    const { count: insertedAthletes } = await prisma.athlete.createMany({ data: athletes, skipDuplicates: true });

    log.extend(absoluteCurrent.toString())('inserted %d athletes', insertedAthletes);
    insert.text = `inserted ${absoluteCurrent}/${absoluteTotal} athletes`;
  }
  insert.succeed('inserted ' + fetchedAthlete.length + ' athletes');
}

async function importCategories(prisma: PrismaClient): Promise<void> {
  const token = await getToken();
  log('getting categories');

  const fetch = ora('fetching categories').start();
  const { data: fetchedAthlete } = await axios.get<any[]>(
    `${process.env.REMOTE_API_URL}/api/category/all`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );
  fetch.succeed('fetched ' + fetchedAthlete.length + ' categories');

  const insert = ora('inserting categories').start();
  for (let current = 0; current < fetchedAthlete.length; current += chunkSize) {
    const absoluteCurrent: number = Math.round(current / chunkSize);
    const absoluteTotal: number = Math.round(fetchedAthlete.length / chunkSize);
    log('saving %d/%d categories', absoluteCurrent, absoluteTotal);
    const chunk = fetchedAthlete.slice(current, current + chunkSize);
    const { count: insertedCategories } = await prisma.category.createMany({
      data: chunk.map((category) => ({
        id: category.id ?? undefined,
        federation: category.federation ?? undefined,
        name: category.name ?? undefined,
        abbr: category.abbr ?? undefined,
        ageMin: category.ageMin ?? undefined,
        ageMax: category.ageMax ?? undefined,
        changeMin: category.changeMin ?? undefined,
        changeMax: category.changeMax ?? undefined,
        gender: category.gender ?? undefined,
        nationalCode: category.nationalCode ?? undefined,
        sortOrder: category.sortOrder ?? undefined,
        categoryId: category.categoryId ?? undefined,
        fedinsidelabel: category.fedinsidelabel ?? undefined,
      })), skipDuplicates: true,
    });

    log.extend(absoluteCurrent.toString())('inserted %d categories', insertedCategories);
    insert.text = `inserted ${absoluteCurrent}/${absoluteTotal} categories`;
  }

  insert.succeed('inserted ' + fetchedAthlete.length + ' categories');
}

async function importResults(client: PrismaClient): Promise<void> {
  const logger = log.extend('results');

  logger('fetching all athletes');
  const athletes = await client.athlete.findMany({take: 3000});
  logger('fetched %d athletes', athletes.length);

  const spinner = ora('fetching and inserting results').start();
  let done = 0;
  const total = athletes.length;
  await Promise.all(athletes.map(async (athlete) => {
    logger('getting results for %s %s', athlete.firstname, athlete.lastname);
    const result = await axios.get(`${process.env.REMOTE_API_URL}/api/athlete/${athlete.liveId}`);
    logger('got %d results for %s %s', result.data.results.length, athlete.firstname, athlete.lastname);

    logger('inserting events');
    await client.event.createMany({
      data: result.data.results.filter((r: any) => r?.event).map((result: any) => ({
        id: result.event.id ?? undefined,
        eventNumber: result.event.eventNumber ?? undefined,
        name: result.event.name ?? undefined,
        date_start: result.event.date_start ?? undefined,
        date_end: result.event.date_end ?? undefined,
        start_time: result.event.start_time ?? undefined,
        championship: result.event.championship ?? undefined,
        season: result.event.season ?? undefined,
        facility: result.event.facility ? JSON.stringify(result.event.facility) : undefined,
        responsible: result.event.responsible ? JSON.stringify(result.event.responsible) : undefined,
        responsible2: result.event.responsible2 ? JSON.stringify(result.event.responsible2) : undefined,
        type: result.event.type ?? undefined,
        chrono: result.event.chrono ?? undefined,
        organizationId: result.event.organizationId ?? undefined,
      })),
      skipDuplicates: true,
    });

    logger('inserting event types');
    await client.eventType.createMany({
      data: result.data.results.filter((r: any) => r?.eventType).map((result: any) => ({
        id: result.eventType.id ?? undefined,
        venue: result.eventType.venue ?? undefined,
        distance: result.eventType.distance ?? undefined,
        wind_mode: result.eventType.wind_mode ?? undefined,
        wind_time: result.eventType.wind_time ?? undefined,
        precision: result.eventType.precision ?? undefined,
        handtime_diff: result.eventType.handtime_diff ?? undefined,
        nb_athletes: result.eventType.nb_athletes ?? 1,
        implement: result.eventType.implement ?? undefined,
        hurdles_nb: result.eventType.hurdles_nb ?? undefined,
        hurdles_first: result.eventType.hurdles_first ?? undefined,
        hurdles_interval: result.eventType.hurdles_interval ?? undefined,
        hurdles_last: result.eventType.hurdles_last ?? undefined,
        abbr: result.eventType.abbr ?? undefined,
        name_fr: result.eventType.name_fr ?? undefined,
        name_nl: result.eventType.name_nl ?? undefined,
        name_de: result.eventType.name_de ?? undefined,
        name_en: result.eventType.name_en ?? undefined,
        low2high: result.eventType.low2high ?? undefined,
        national_code: result.eventType.national_code ?? undefined,
        sort_order: result.eventType.sort_order ?? undefined,
        type_id: result.eventType.type_id ?? undefined,
        result_type: result.eventType.result_type ?? undefined,
        discipline_group: result.eventType.discipline_group ?? undefined,
      })),
      skipDuplicates: true,
    });

    logger('inserting results');
    const { count: inserted } = await client.result.createMany({
      data: result.data.results.map((result: any) => ({
        id: result.id ?? undefined,
        eventNumber: result.eventNumber ?? undefined,
        name: result.name ?? undefined,
        abbr: result.abbr ?? undefined,
        type: result.type ?? undefined,
        date: result.date ?? undefined,
        validation: result.validation ?? undefined,
        xmlId: result.xmlId ?? undefined,
        round: result.round ? JSON.stringify(result.round) : undefined,
        heat: result.heat ? JSON.stringify(result.heat) : undefined,
        result: result.result ? JSON.stringify(result.result) : undefined,
        eventCategory: result.eventCategory ? JSON.stringify(result.eventCategory) : undefined,
        athleteCategory: result.athleteCategory ? JSON.stringify(result.athleteCategory) : undefined,
        eventId: result.event?.id ?? undefined,
        eventTypeId: result.eventTypeId ?? result.eventType?.id ?? undefined,
        categoryId: result.categoryId ?? undefined,
      })), skipDuplicates: true,
    });
    logger('inserted %d results for %s %s', inserted, athlete.firstname, athlete.lastname);
    done += 1;
    spinner.text = `inserted ${done}/${total} results`;
    return result.data;
  }));

  spinner.succeed('inserted all results');
}


const client = new PrismaClient();

async function main() {
  // await importOrgs(client);
  // await importAthletes(client);
  // await importCategories(client);
  await importResults(client);
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await client.$disconnect();
  });
