import { TestInfrastructure } from './support/infrastructure';
describe('PostgreSQL transactions', () => {
  let infra: TestInfrastructure;
  beforeAll(async () => {
    infra = await TestInfrastructure.create();
    await infra.db.query('CREATE TABLE checks (value integer UNIQUE)');
  });
  afterAll(async () => {
    await infra?.close();
  });
  test('commits related writes and rolls them back together on constraint failure', async () => {
    await infra.db.transaction(async (c) => {
      await c.query('INSERT INTO checks VALUES (1)');
      await c.query('INSERT INTO checks VALUES (2)');
    });
    await expect(
      infra.db.transaction(async (c) => {
        await c.query('INSERT INTO checks VALUES (3)');
        await c.query('INSERT INTO checks VALUES (1)');
      }),
    ).rejects.toMatchObject({ code: '23505' });
    expect(
      (await infra.db.query('SELECT value FROM checks ORDER BY value')).rows,
    ).toEqual([{ value: 1 }, { value: 2 }]);
  });
});
