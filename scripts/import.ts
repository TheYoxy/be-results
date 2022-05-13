import { Prisma, Athlete, Organization, PrismaClient } from '@prisma/client';
import axios, { AxiosResponse } from 'axios';
import debug from 'debug';
import 'dotenv';

const prisma = new PrismaClient();

const log = debug('app:scripts:import');

(async function () {
  await prisma.$connect();

  for (let i = 'a'.charCodeAt(0); i <= 'c'.charCodeAt(0); i++) {
    const char = String.fromCharCode(i);
    const config = {
      method: 'get',
      url: 'https://www.beathletics.be/api/search/public/' + char,
      headers: {
        'Accept': 'application/json',
      },
    };
    log('fetching %s', char);
    const response: AxiosResponse<{ athletes: (Athlete & { organization: Organization })[], events: [] }> = await axios(config);
    log('found %s: %d', char, response.data.athletes.length);

    const chunkSize = 1000;
    for (let current = 0; current < response.data.athletes.length; current += chunkSize) {
      log('saving %d/%d athletes', current + 1, response.data.athletes.length);
      const chunk = response.data.athletes.slice(current, current + chunkSize);
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
      const orgs = chunk.filter(athletes => athletes.organization).map(({ organization }) => ({
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
      const { count: insertedAthletes } = await prisma.athlete.createMany({ data: athletes, skipDuplicates: true });
      log.extend((current / chunkSize).toString())('inserted %d athletes and %d organizations', insertedAthletes, insertedOrgs);

      log('saved %d/%d athletes', current, response.data.athletes.length);
    }
  }
})().finally(async () => {
  await prisma.$disconnect();
});
